import datetime
import decimal
import logging
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Union

from typing_extensions import override

from unitycatalog.ai.core.client import BaseFunctionClient, FunctionExecutionResult
from unitycatalog.ai.core.paged_list import PagedList
from unitycatalog.ai.core.utils.callable_utils import ReturnParamFormat, parse_callable
from unitycatalog.ai.core.utils.type_utils import column_type_to_python_type
from unitycatalog.ai.core.utils.validation_utils import FullFunctionName

# TODO: namespace to be changed
from unitycatalog.python import InternalServerError, Unitycatalog
from unitycatalog.python.types.function_create_params import FunctionInfo as CreateFunctionInfo
from unitycatalog.python.types.function_create_params import (
    FunctionInfoInputParams,
    FunctionInfoInputParamsParameter,
)
from unitycatalog.python.types.function_info import FunctionInfo, InputParamsParameter

ALLOWED_DATA_TYPES = {
    "BOOLEAN",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "STRING",
    "BINARY",
    "DECIMAL",
    "INTERVAL",
    "ARRAY",
    "STRUCT",
    "MAP",
    "CHAR",
    # below types are not supported in python execution so we excluded them
    # "NULL",
    # "USER_DEFINED_TYPE",
    # "TABLE_TYPE",
}

SQL_TYPE_TO_PYTHON_TYPE_MAPPING_UC_OSS = {
    "ARRAY": (list, tuple),
    "BINARY": bytes,
    "BOOLEAN": bool,
    # tinyint type
    "BYTE": int,
    "CHAR": str,
    "DATE": datetime.date,
    "DECIMAL": decimal.Decimal,
    "DOUBLE": float,
    "FLOAT": float,
    "INT": int,
    "INTERVAL": datetime.timedelta,
    "LONG": int,
    "MAP": dict,
    "SHORT": int,
    "STRING": str,
    "STRUCT": dict,
    "TIMESTAMP": (datetime.datetime, str),
    "TIMESTAMP_NTZ": (datetime.datetime, str),
}

_logger = logging.getLogger(__name__)


class UnitycatalogFunctionClient(BaseFunctionClient):
    """
    Unity Catalog function calling client
    """

    def __init__(self, uc: Optional[Unitycatalog] = None, **kwargs: Any) -> None:
        if uc is None:
            uc = Unitycatalog()
        if not isinstance(uc, Unitycatalog):
            raise ValueError("client must be an instance of Unitycatalog")
        self.uc = uc
        # host all python functions within this cache
        self.func_cache = {}
        super().__init__()

    @override
    def create_function(
        self,
        *,
        function_name: str,
        routine_definition: str,
        data_type: str,
        parameters: Optional[List[Union[FunctionInfoInputParamsParameter, Dict[str, str]]]] = None,
        timeout: Optional[float] = None,
        replace: bool = False,
        **kwargs,
    ) -> FunctionInfo:
        """
        Create a function in Unity Catalog.

        Args:
            function_name: The full function name in the format <catalog_name>.<schema_name>.<function_name>.
            routine_definition: The function definition.
            data_type: The return data type. Allowed values are:
                "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
                "TIMESTAMP_NTZ", "STRING", "BINARY", "DECIMAL", "INTERVAL", "ARRAY", "STRUCT",
                "MAP", "CHAR"
            parameters: The input parameters.
            timeout: The timeout in seconds.
            replace: Whether to replace the function if it already exists. Default to False.
        """
        function_name = FullFunctionName.validate_full_function_name(function_name)
        parameters = [validate_input_parameter(param) for param in parameters]
        if data_type not in ALLOWED_DATA_TYPES:
            raise ValueError(
                f"Invalid data_type {data_type}, allowed values are {ALLOWED_DATA_TYPES}."
            )
        if replace and self.get_function(function_name, timeout=timeout):
            _logger.info(f"Function {function_name} already exists, replacing it.")
            self.delete_function(function_name, timeout=timeout)
        function_info = CreateFunctionInfo(
            catalog_name=function_name.catalog,
            schema_name=function_name.schema,
            name=function_name.function,
            input_params=FunctionInfoInputParams(parameters=parameters),
            data_type=data_type,
            routine_body="EXTERNAL",
            routine_definition=routine_definition,
            security_type="DEFINER",
            parameter_style="S",
            external_language="PYTHON",
            sql_data_access="NO_SQL",
        )
        return self.uc.functions.create(function_info=function_info, timeout=timeout, **kwargs)

    @override
    def create_python_function(
        self, *, func: Callable[..., Any], catalog: str, schema: str, replace: bool = False
    ) -> FunctionInfo:
        if not callable(func):
            raise ValueError("The provided function is not callable.")
        parsed_callable = parse_callable(
            func, return_format=ReturnParamFormat.FUNCTION_INFO_INPUT_PARAMETER_DICT
        )
        return self.create_function(
            function_name=f"{catalog}.{schema}.{parsed_callable.function_name}",
            routine_definition=parsed_callable.function_body,
            data_type=parsed_callable.return_type,
            parameters=parsed_callable.input_params,
            replace=replace,
        )

    @override
    def get_function(self, function_name: str, timeout: Optional[float] = None) -> FunctionInfo:
        try:
            return self.uc.functions.retrieve(function_name, timeout=timeout)
        except InternalServerError as e:
            _logger.warning(
                f"Failed to retrieve function {function_name} from Unity Catalog, the function may not exist. "
                f"Exception: {e}"
            )

    @override
    def list_functions(
        self,
        catalog: str,
        schema: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> PagedList[FunctionInfo]:
        resp = self.uc.functions.list(
            catalog_name=catalog,
            schema_name=schema,
            max_results=max_results,
            page_token=page_token,
            timeout=timeout,
        )
        return PagedList(resp.functions, resp.next_page_token)

    @override
    def _validate_param_type(self, value: Any, param_info: InputParamsParameter) -> None:
        value_python_type = column_type_to_python_type(
            param_info.type_name, mapping=SQL_TYPE_TO_PYTHON_TYPE_MAPPING_UC_OSS
        )
        if not isinstance(value, value_python_type):
            raise ValueError(
                f"Parameter {param_info.name} should be of type {param_info.type_name} "
                f"(corresponding python type {value_python_type}), but got {type(value)}"
            )
        validate_param(value, param_info.type_name, param_info.type_text)

    @override
    def _execute_uc_function(
        self, function_info: FunctionInfo, parameters: Dict[str, Any], **kwargs: Any
    ) -> FunctionExecutionResult:
        if function_info.name in self.func_cache:
            result = self.func_cache[function_info.name](**parameters)

            return FunctionExecutionResult(format="SCALAR", value=str(result))
        else:
            python_function = dynamic_construct_python_function(function_info)
            exec(python_function.function_def, self.func_cache)
            try:
                # use local variables to avoid positional arguments error
                result = eval(python_function.function_head, self.func_cache, parameters)

                # TODO: do we need to enforce result type to be the same as function_info.data_type?

                return FunctionExecutionResult(format="SCALAR", value=str(result))
            except Exception as e:
                return FunctionExecutionResult(error=str(e))

    @override
    def delete_function(
        self,
        function_name: str,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Delete a function by its full name.

        Args:
            function_name: The full name of the function to delete.
                It should be in the format of "catalog.schema.function_name".
            timeout: The timeout in seconds.
        """
        self.uc.functions.delete(function_name, timeout=timeout)

    @override
    def to_dict(self):
        # TODO: check what other attributes need to be saved
        return {k: getattr(self, k) for k in ["base_url"] if getattr(self, k) is not None}


class FunctionDefinition(NamedTuple):
    function_def: str
    function_head: str


def dynamic_construct_python_function(function_info: FunctionInfo) -> FunctionDefinition:
    """
    Construct a python function from the function info
    """
    param_names = [param.name for param in function_info.input_params.parameters]
    function_head = f"{function_info.name}({', '.join(param_names)})"
    func_def = f"def {function_head}:\n"
    if function_info.routine_body == "EXTERNAL":
        for line in function_info.routine_definition.split("\n"):
            func_def += f"    {line}\n"
    else:
        raise NotImplementedError(f"routine_body {function_info.routine_body} not supported")

    return FunctionDefinition(function_def=func_def, function_head=function_head)


def validate_input_parameter(
    parameter: Union[FunctionInfoInputParamsParameter, Dict[str, str]],
) -> FunctionInfoInputParamsParameter:
    """
    Validate the input parameter and convert it to FunctionInfoInputParamsParameter.

    Args:
        parameter: The input parameter to validate. If it is a dict, it will be converted to FunctionInfoInputParamsParameter.

    Returns:
        The validated FunctionInfoInputParamsParameter.
    """
    if isinstance(parameter, dict):
        parameter = FunctionInfoInputParamsParameter(**parameter)
    elif not isinstance(parameter, FunctionInfoInputParamsParameter):
        raise TypeError(
            f"Input parameter should be either a dict or an instance of "
            f"FunctionInfoInputParamsParameter, but got {type(parameter)}"
        )
    # NOTE: position is not required, even position collision is allowed
    if missing_keys := {"name", "type_name", "type_text"} - parameter.keys():
        raise ValueError(f"Missing keys in input parameter {parameter}: {missing_keys}")
    type_name = parameter["type_name"]
    # parameters extracted from python type hints could contain internal types as well
    # we don't need internal types in `type_name` field
    for complex_type_name in ["ARRAY", "MAP", "STRUCT", "DECIMAL", "INTERVAL"]:
        if type_name.startswith(complex_type_name):
            parameter["type_name"] = complex_type_name
            return parameter
    if type_name not in ALLOWED_DATA_TYPES:
        raise ValueError(
            f"Invalid type_name {type_name} in input parameter "
            f"{parameter}, allowed values are {ALLOWED_DATA_TYPES}."
        )
    return parameter


def validate_param(param: Any, column_type: str, param_type_text: str) -> None:
    """
    Validate the parameter against the parameter info.

    Args:
        param: The parameter to validate.
        column_type: The column type name.
        param_type_text: The parameter type text.
    """
    if (
        column_type == "INTERVAL"
        and isinstance(param, datetime.timedelta)
        and param_type_text != "interval day to second"
    ):
        raise ValueError(
            f"Invalid interval type text: {param_type_text}, expecting 'interval day to second', "
            "python timedelta can only be used for day-time interval."
        )
