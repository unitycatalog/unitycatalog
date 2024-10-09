import json
import logging
import threading
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Dict, Literal, Optional

from ucai.core.paged_list import PagedList

_logger = logging.getLogger(__name__)

_uc_function_client = None
_client_lock = threading.Lock()


@dataclass
class FunctionExecutionResult:
    """
    Result of executing a function.
    Value is always string, even if the function returns a scalar or a collection.
    """

    error: Optional[str] = None
    format: Optional[Literal["SCALAR", "CSV"]] = None
    value: Optional[str] = None
    truncated: Optional[bool] = None

    def to_json(self) -> str:
        data = {k: v for (k, v) in self.__dict__.items() if v is not None}
        return json.dumps(data)


class BaseFunctionClient(ABC):
    """
    Base class for uc function calling client
    """

    def __init__(self, client: Any = None, **kwargs: Any) -> None:  # noqa: ARG002
        self._lock = threading.Lock()

    @abstractmethod
    def create_function(self, *args: Any, **kwargs: Any) -> Any:
        """Create a function"""

    @abstractmethod
    def create_python_function(
        self, *, func: Callable[..., Any], catalog: str, schema: str, replace: bool = False
    ) -> Any:
        """
        Create a Python function

        Args:
            func: A Python Callable object to be converted into a UC function.
            catalog: The catalog name.
            schema: The schema name.
            replace: Whether to replace the function if it already exists. Defaults to False.

        Returns:
            The UC function information metadata for the configured UC implementation.
        """

    @abstractmethod
    def get_function(self, function_name: str, **kwargs: Any) -> Any:
        """
        Get a function by its name.

        Args:
            function_name: The name of the function to get.
            kwargs: additional key-value pairs to include when getting the function.
        """

    @abstractmethod
    def list_functions(
        self,
        catalog: str,
        schema: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> PagedList[Any]:
        """
        List functions in a catalog and schema.

        Args:
            catalog: The catalog name.
            schema: The schema name.
            max_results: The maximum number of functions to return. Defaults to None.
            page_token: The token for the next page. Defaults to None.

        Returns:
            PageList: The paginated list of function infos, the type of the function
                info is determined by the client implementation.
        """

    def validate_input_params(self, input_params: Any, parameters: Dict[str, Any]) -> None:
        """
        Validate passed parameters against the function's input parameters definition.

        Args:
            input_params: The function's input parameters definition. It should be
                of type InputParams in unitycatalog, or FunctionParameterInfos in databricks.
            parameters: The parameters to validate.
        """
        parameters = deepcopy(parameters)
        if input_params and input_params.parameters:
            invalid_params: Dict[str, str] = {}
            for param in input_params.parameters:
                if param.name in parameters:
                    try:
                        self._validate_param_type(parameters[param.name], param)
                    except ValueError as e:
                        invalid_params[param.name] = str(e)
                elif param.parameter_default is None:
                    raise ValueError(f"Parameter {param.name} is required but not provided.")
            if invalid_params:
                raise ValueError(f"Invalid parameters provided: {invalid_params}.")
            if extra_params := parameters.keys() - {
                param.name for param in input_params.parameters
            }:
                raise ValueError(
                    f"Extra parameters provided that are not defined in the function's input parameters: {extra_params}."
                )
        elif parameters:
            raise ValueError(
                f"Function does not have input parameters, but parameters {parameters} were provided."
            )

    @abstractmethod
    def _validate_param_type(self, value: Any, param_info: Any) -> None:
        """
        Validate the type of a parameter against the function's input parameter info.

        Args:
            value (Any): The value of the parameter.
            param_info (Any): The parameter info.
        """

    def execute_function(
        self, function_name: str, parameters: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> FunctionExecutionResult:
        """
        Execute a UC function by name with the given parameters.

        Args:
            function_name: The name of the function to execute.
            parameters: The parameters to pass to the function. Defaults to None.
            kwargs: additional key-value pairs to include when executing the function.

        Returns:
            FunctionExecutionResult: The result of executing the function.
        """
        with self._lock:
            function_info = self.get_function(function_name, **kwargs)
            parameters = parameters or {}
            self.validate_input_params(function_info.input_params, parameters)
            return self._execute_uc_function(function_info, parameters, **kwargs)

    @abstractmethod
    def _execute_uc_function(
        self, function_info: Any, parameters: Dict[str, Any], **kwargs: Any
    ) -> FunctionExecutionResult:
        """
        Internal logic for executing a UC function.
        """

    @abstractmethod
    def to_dict(self):
        """
        Store the client configuration in a dictionary.
        Sensitive information should be excluded.
        """


# TODO: update BaseFunctionClient to Union[BaseFunctionClient, AsyncBaseFunctionClient] after async client is supported
def get_uc_function_client() -> Optional[BaseFunctionClient]:
    global _uc_function_client

    with _client_lock:
        if _uc_function_client is None:
            _logger.warning("UC function client is not set.")
        return _uc_function_client


def set_uc_function_client(client: BaseFunctionClient) -> None:
    global _uc_function_client

    if client and not isinstance(client, BaseFunctionClient):
        raise ValueError("client must be an instance of BaseFunctionClient")

    with _client_lock:
        _uc_function_client = client
