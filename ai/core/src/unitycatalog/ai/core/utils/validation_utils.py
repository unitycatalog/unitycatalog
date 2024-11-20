import base64
import datetime
import warnings
from typing import TYPE_CHECKING, Any, NamedTuple

from unitycatalog.ai.core.utils.type_utils import is_time_type

if TYPE_CHECKING:
    from databricks.sdk.service.catalog import FunctionInfo


class FullFunctionName(NamedTuple):
    catalog: str
    schema: str
    function: str

    def __str__(self) -> str:
        return f"{self.catalog.strip('`')}.{self.schema.strip('`')}.{self.function.strip('`')}"

    def to_tool_name(self) -> str:
        return str(self).replace(".", "__")

    @classmethod
    def validate_full_function_name(cls, function_name: str) -> "FullFunctionName":
        """
        Validate the full function name follows the format <catalog_name>.<schema_name>.<function_name>.

        Args:
            function_name: The full function name.

        Returns:
            FullFunctionName: The parsed full function name.
        """
        splits = function_name.split(".")
        if len(splits) != 3:
            raise ValueError(
                f"Invalid function name: {function_name}, expecting format <catalog_name>.<schema_name>.<function_name>."
            )
        return cls(catalog=splits[0], schema=splits[1], function=splits[2])


def is_base64_encoded(s: str) -> bool:
    try:
        base64.b64decode(s, validate=True)
        return True
    except (base64.binascii.Error, ValueError):
        return False


def validate_param(param: Any, column_type: str, param_type_text: str) -> None:
    """
    Validate the parameter against the parameter info.

    Args:
        param (Any): The parameter to validate.
        column_type (str): The column type name.
        param_type_text (str): The parameter type text.
    """
    if is_time_type(column_type) and isinstance(param, str):
        try:
            datetime.datetime.fromisoformat(param)
        except ValueError as e:
            raise ValueError(f"Invalid datetime string: {param}, expecting ISO format.") from e
    elif column_type == "INTERVAL":
        # only day-time interval is supported, no year-month interval
        if isinstance(param, datetime.timedelta) and param_type_text != "interval day to second":
            raise ValueError(
                f"Invalid interval type text: {param_type_text}, expecting 'interval day to second', "
                "python timedelta can only be used for day-time interval."
            )
        # Only DAY TO SECOND is supported in python udf
        # rely on the SQL function for checking the interval format
        elif isinstance(param, str) and not (
            param.startswith("INTERVAL") and param.endswith("DAY TO SECOND")
        ):
            raise ValueError(
                f"Invalid interval string: {param}, expecting format `INTERVAL '[+|-] d[...] [h]h:[m]m:[s]s.ms[ms][ms][us][us][us]' DAY TO SECOND`."
            )
    elif column_type == "BINARY" and isinstance(param, str) and not is_base64_encoded(param):
        # the string value for BINARY column must be base64 encoded
        raise ValueError(
            f"The string input for column type BINARY must be base64 encoded, invalid input: {param}."
        )


def check_function_info(func_info: "FunctionInfo") -> None:
    """
    Checks a FunctionInfo object for missing parameter descriptions and a function description.
    If these are missing, issue a warning to instruct users on how beneficial to their GenAI
    applications these properties are.

    Parameters:
        func_info (FunctionInfo): The function information to check.

    Uses:
        warnings.warn to issue warnings if any parameters or the function itself lack descriptions.
    """
    if func_info.input_params:
        params_with_no_description = []

        for param_info in func_info.input_params.parameters:
            if not param_info.comment:
                params_with_no_description.append(param_info.name)

        if params_with_no_description:
            warnings.warn(
                f"The following parameters do not have descriptions: {', '.join(params_with_no_description)} for the function {func_info.full_name}. "
                "Using Unity Catalog functions that do not have parameter descriptions limits the functionality "
                "for an LLM to understand how to call your function. To improve tool calling accuracy, provide "
                "verbose parameter descriptions that fully explain what the expected usage of the function arguments are.",
                UserWarning,
                stacklevel=2,
            )

    if not func_info.comment:
        warnings.warn(
            f"The function {func_info.name} does not have a description. "
            "Using Unity Catalog functions that do not have function descriptions limits the functionality "
            "for an LLM to understand when it is appropriate to call your function as a tool and how to properly interface with the function. "
            "Update your function's description with a verbose entry in the 'comments' parameter to improve the usage characterstics of this function "
            "as a tool.",
            UserWarning,
            stacklevel=2,
        )
