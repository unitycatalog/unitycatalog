import asyncio
import atexit
import datetime
import decimal
import logging
from enum import Enum
from functools import lru_cache, wraps
from typing import Any, Callable, Dict, List, Optional, Union

import nest_asyncio
from typing_extensions import override

from unitycatalog.ai.core.client import BaseFunctionClient, FunctionExecutionResult
from unitycatalog.ai.core.paged_list import PagedList
from unitycatalog.ai.core.utils.callable_utils_oss import generate_function_info
from unitycatalog.ai.core.utils.type_utils import column_type_to_python_type
from unitycatalog.ai.core.utils.validation_utils import (
    FullFunctionName,
    validate_function_name_length,
)
from unitycatalog.client import (
    ApiClient,
    CatalogInfo,
    CatalogsApi,
    CreateCatalog,
    CreateFunction,
    CreateFunctionRequest,
    CreateSchema,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
    FunctionsApi,
    SchemaInfo,
    SchemasApi,
)
from unitycatalog.client.exceptions import NotFoundException, ServiceException

nest_asyncio.apply()

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


def syncify_method(sync_method):
    @wraps(sync_method)
    def wrapper(self, *args, **kwargs):
        async_method_name = f"{sync_method.__name__}_async"
        async_method = getattr(self, async_method_name)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # NB: For standard pythons script execution environments, a new asyncio event loop
            # can be created to handle the async call via a synchronous wrapper.
            # This cannot run in environments that already have a running asyncio event loop
            # such as Jupyter/Ipython/Idle kernels since multiple event loops cannot run in the
            # same thread.
            return asyncio.run(async_method(*args, **kwargs))
        else:
            # NB: Jupyter kernels use a persistent asyncio loop to handle the active REPL
            # context for which we can access the running loop to handle our future.
            return loop.run_until_complete(async_method(*args, **kwargs))

    return wrapper


class UnitycatalogClient:
    def __init__(self, api_client: ApiClient):
        self.api_client = api_client
        self.functions_client = FunctionsApi(api_client=api_client)
        self.catalogs_client = CatalogsApi(api_client=api_client)
        self.schemas_client = SchemasApi(api_client=api_client)

        # Clean up the ApiClient instance for aiohttp to ensure that we're not leaking resources
        # and preventing Python's GC operation as well as to ensure that multiple instances of
        # this client are not present within a thread (eliminate a potential memory leak).
        atexit.register(self.close)

    async def close_async(self):
        """Asynchronously close the underlying ApiClient."""
        if getattr(self, "_closed", None):
            return
        self._closed = True
        try:
            await self.api_client.close()
            _logger.info("ApiClient successfully closed.")
        except Exception as e:
            _logger.error(f"Error while closing ApiClient: {e}")

    def close(self):
        """Synchronously close the underlying ApiClient."""
        if getattr(self, "_closed", None):
            return
        self._closed = True
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.close_async())
            else:
                loop.run_until_complete(self.close_async())
        except Exception as e:
            _logger.error(f"Error while closing ApiClient: {e}")

    def __enter__(self):
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close the ApiClient."""
        self.close()

    async def create_catalog_async(
        self,
        name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> CatalogInfo:
        """
        Create a catalog in Unity Catalog asynchronously.

        Args:
            name: The name of the catalog to create.
            comment: An optional comment (description) for the catalog.
            properties: Optional dictionary of properties for the catalog. Property options are validated
                through the REST interface and can be discovered via the Unity Catalog Server API documentation.
            **kwargs: Additional keyword arguments for the creation call (defined as overridable private methods)
                the optional values (i.e., _request_timeout, _headers, _host_index) can be found in the
                Unity Catalog Server API documentation. These parameters are in an experimental state and are
                subject to change.

        Returns:
            The created CatalogInfo object.

        Raises:
            ValueError: If the catalog already exists.
        """

        catalog = None
        try:
            catalog = await self._get_catalog(name)
        except NotFoundException:
            pass

        if catalog:
            _logger.info(f"The catalog '{name}' already exists.")
            return catalog

        catalog_create_request = CreateCatalog(
            name=name,
            comment=comment,
            properties=properties,
        )

        return await self.catalogs_client.create_catalog(
            create_catalog=catalog_create_request, **kwargs
        )

    @syncify_method
    def create_catalog(
        self,
        name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> CatalogInfo:
        """
        Create a catalog in Unity Catalog.

        This is a synchronous version of `create_catalog_async`.

        Args:
            name: The name of the catalog to create.
            comment: An optional comment (description) for the catalog.
            properties: Optional dictionary of properties for the catalog. Property options are validated
                through the REST interface and can be discovered via the Unity Catalog Server API documentation.
            **kwargs: Additional keyword arguments for the creation call (defined as overridable private methods)
                the optional values (i.e., _request_timeout, _headers, _host_index) can be found in the
                Unity Catalog Server API documentation. These parameters are in an experimental state and are
                subject to change.

        Returns:
            The created CatalogInfo object.
        """

        pass

    async def create_schema_async(
        self,
        name: str,
        catalog_name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SchemaInfo:
        """
        Create a schema within a catalog in Unity Catalog asynchronously.

        Args:
            name: The name of the schema to create.
            catalog_name: The name of the catalog where the schema will be created.
            comment: An optional comment (description) for the schema.
            properties: Optional dictionary of properties for the schema. Property options are validated
                through the REST interface and can be discovered through the Unity Catalog Server API
                documentation.
            **kwargs: Additional keyword arguments for the creation call (defined as overridable private methods)
                the optional values (i.e., _request_timeout, _headers, _host_index) can be found in the
                Unity Catalog Server API documentation. These parameters are in an experimental state and are
                subject to change.

        Returns:
            The created SchemaInfo object.

        Raises:
            ValueError: If the specified catalog does not exist.
        """

        try:
            await self._get_catalog(catalog_name)
        except NotFoundException as e:
            raise ValueError(
                f"The Catalog that you specified: '{catalog_name}' does not exist on this server."
            ) from e

        schema = None
        try:
            schema = await self._get_schema(name=name, catalog_name=catalog_name)
        except NotFoundException:
            pass

        if schema:
            _logger.info(
                f"The schema '{name}' already exists in the catalog '{catalog_name}'",
            )
            return schema

        schema_create_request = CreateSchema(
            name=name,
            catalog_name=catalog_name,
            comment=comment,
            properties=properties,
        )

        return await self.schemas_client.create_schema(
            create_schema=schema_create_request, **kwargs
        )

    @syncify_method
    def create_schema(
        self,
        name: str,
        catalog_name: str,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> SchemaInfo:
        """
        Create a schema within a catalog in Unity Catalog.

        This is a synchronous version of `create_schema_async`.

        Args:
            name: The name of the schema to create.
            catalog_name: The name of the catalog where the schema will be created.
            comment: An optional comment (description) for the schema.
            properties: Optional dictionary of properties for the schema. Property options are validated
                through the REST interface and can be discovered through the Unity Catalog Server API
                documentation.
            **kwargs: Additional keyword arguments for the creation call (defined as overridable private methods)
                the optional values (i.e., _request_timeout, _headers, _host_index) can be found in the
                Unity Catalog Server API documentation. These parameters are in an experimental state and are
                subject to change.

        Returns:
            The created SchemaInfo object.
        """

        pass

    async def _get_schema(self, name: str, catalog_name: str) -> SchemaInfo:
        """ """
        return await self.schemas_client.get_schema(full_name=f"{catalog_name}.{name}")

    async def _get_catalog(self, name: str) -> CatalogInfo:
        """ """
        return await self.catalogs_client.get_catalog(name=name)


class UnitycatalogFunctionClient(BaseFunctionClient):
    """
    Unity Catalog function client for managing and executing functions in Unity Catalog OSS.
    """

    def __init__(self, api_client: ApiClient, **kwargs: Any) -> None:
        """
        Initialize the UnitycatalogFunctionClient.

        Args:
            api_client: An instance of unitycatalog.client.ApiClient that has been constructed with the desired Configuration.
            **kwargs: Additional keyword arguments.
        """

        if not isinstance(api_client, ApiClient):
            raise ValueError(
                "The 'api_client' must be an instance of unitycatalog.client.ApiClient"
            )

        self.uc = UnitycatalogClient(api_client)
        self.func_cache = {}
        super().__init__()

        # Clean up the ApiClient instance for aiohttp to ensure that we're not leaking resources
        # and preventing Python's GC operation as well as to ensure that multiple instances of
        # this client are not present within a thread (eliminate a potential memory leak).
        atexit.register(self.close)

    async def close_async(self):
        """Asynchronously close the underlying ApiClient."""
        if getattr(self, "_closed", None):
            return
        self._closed = True
        try:
            await self.uc.api_client.close()
            _logger.info("ApiClient successfully closed.")
        except Exception as e:
            _logger.error(f"Error while closing ApiClient: {e}")

    def close(self):
        """Synchronously close the underlying ApiClient."""
        if getattr(self, "_closed", None):
            return
        self._closed = True
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.ensure_future(self.close_async())
            else:
                loop.run_until_complete(self.close_async())
        except Exception as e:
            _logger.error(f"Error while closing ApiClient: {e}")

    def __enter__(self):
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager and close the ApiClient."""
        self.close()

    async def create_function_async(
        self,
        *,
        function_name: str,
        routine_definition: str,
        data_type: str,
        full_data_type: str,
        comment: str,
        parameters: Optional[List[Union[FunctionParameterInfo, Dict[str, str]]]] = None,
        properties: Optional[str] = "null",
        timeout: Optional[float] = None,
        replace: bool = False,
    ) -> FunctionInfo:
        """
        Create a function in Unity Catalog asynchronously.

        Args:
            function_name: The full function name in the format <catalog_name>.<schema_name>.<function_name>.
            routine_definition: The function definition.
            data_type: The return data type. Allowed values are:
                "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
                "TIMESTAMP_NTZ", "STRING", "BINARY", "DECIMAL", "INTERVAL", "ARRAY", "STRUCT",
                "MAP", "CHAR"
            full_data_type: The full return data type, including inner types for collections, i.e.:
                "ARRAY<STRING>". If the return data type is a scalar, this is the same as the 'data_type'
                field.
            comment: The description of the function.
            parameters: The input parameters.
            properties: JSON-serialized key-value pair map encoded as a string.
            timeout: The timeout in seconds.
            replace: Whether to replace the function if it already exists. Defaults to False.

        Returns:
            The created FunctionInfo object.
        """

        validate_function_name_length(function_name=function_name)

        function_name = FullFunctionName.validate_full_function_name(function_name)
        parameters = [validate_input_parameter(param) for param in parameters]
        if data_type not in ALLOWED_DATA_TYPES:
            raise ValueError(
                f"Invalid data_type {data_type}, allowed values are {ALLOWED_DATA_TYPES}."
            )

        try:
            await self.get_function_async(str(function_name), timeout=timeout)
            if replace:
                _logger.info(f"Function {function_name} already exists, replacing it.")
                await self.delete_function_async(str(function_name), timeout=timeout)
            else:
                raise ValueError(
                    f"Function {function_name} already exists. "
                    f"Set replace=True to overwrite it."
                )
        except ServiceException:
            pass

        function_info = CreateFunction(
            catalog_name=function_name.catalog,
            schema_name=function_name.schema,
            name=function_name.function,
            specific_name=function_name.function,
            input_params=FunctionParameterInfos(parameters=parameters),
            data_type=data_type,
            full_data_type=full_data_type,
            routine_body="EXTERNAL",
            routine_definition=routine_definition,
            security_type="DEFINER",
            parameter_style="S",
            external_language="PYTHON",
            sql_data_access="NO_SQL",
            is_deterministic=True,
            is_null_call=False,
            properties=properties,
            comment=comment,
        )
        function_request = CreateFunctionRequest(function_info=function_info)
        return await self.uc.functions_client.create_function(
            function_request, _request_timeout=timeout
        )

    @override
    @syncify_method
    def create_function(
        self,
        *,
        function_name: str,
        routine_definition: str,
        data_type: str,
        full_data_type: str,
        comment: str,
        parameters: Optional[List[Union[FunctionParameterInfo, Dict[str, str]]]] = None,
        properties: Optional[str] = "null",
        timeout: Optional[float] = None,
        replace: bool = False,
    ) -> FunctionInfo:
        """
        Create a function in Unity Catalog.

        This is a synchronous version of `create_function_async`.

        Args:
            function_name: The full function name in the format <catalog_name>.<schema_name>.<function_name>.
            routine_definition: The function definition.
            data_type: The return data type. Allowed values are:
                "BOOLEAN", "BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE", "DATE", "TIMESTAMP",
                "TIMESTAMP_NTZ", "STRING", "BINARY", "DECIMAL", "INTERVAL", "ARRAY", "STRUCT",
                "MAP", "CHAR"
            full_data_type: The full return data type, including inner types for collections, i.e.:
                "ARRAY<STRING>". If the return data type is a scalar, this is the same as the 'data_type'
                field.
            comment: The description of the function.
            parameters: The input parameters.
            properties: JSON-serialized key-value pair map encoded as a string.
            timeout: The timeout in seconds.
            replace: Whether to replace the function if it already exists. Defaults to False.

        Returns:
            The created FunctionInfo object.
        """

        pass

    async def create_python_function_async(
        self,
        *,
        func: Callable[..., Any],
        catalog: str,
        schema: str,
        replace: bool = False,
        properties: Optional[str] = "null",
        timeout: Optional[float] = None,
    ) -> FunctionInfo:
        """
        Create a Python function in Unity Catalog asynchronously.

        Args:
            func: The Python function to create in Unity Catalog.
            catalog: The catalog name.
            schema: The schema name.
            replace: Whether to replace the function if it already exists. Defaults to False.
            properties: JSON-serialized key-value pair map encoded as a string. Currently serves
                as a reserved field for future functionality. Required in the client API, but defaulted
                to "null" in this API.
            timeout: The timeout in seconds.

        Returns:
            The created FunctionInfo object.
        """

        if not callable(func):
            raise ValueError("The provided function is not callable.")

        callable_info = generate_function_info(func=func)

        function_name = f"{catalog}.{schema}.{callable_info.callable_name}"

        return await self.create_function_async(
            function_name=function_name,
            routine_definition=callable_info.routine_definition,
            data_type=callable_info.data_type,
            full_data_type=callable_info.full_data_type,
            comment=callable_info.comment,
            parameters=callable_info.parameters,
            properties=properties,
            timeout=timeout,
            replace=replace,
        )

    @override
    @syncify_method
    def create_python_function(
        self,
        *,
        func: Callable[..., Any],
        catalog: str,
        schema: str,
        replace: bool = False,
        properties: Optional[str] = "null",
        timeout: Optional[float] = None,
    ) -> FunctionInfo:
        """
        Create a Python function in Unity Catalog.

        This is a synchronous version of `create_python_function_async`.

        Args:
            func: The Python function to create in Unity Catalog.
            catalog: The catalog name.
            schema: The schema name.
            replace: Whether to replace the function if it already exists. Defaults to False.
            properties: JSON-serialized key-value pair map encoded as a string.
            timeout: The timeout in seconds.

        Returns:
            The created FunctionInfo object.
        """

        pass

    async def get_function_async(
        self, function_name: str, timeout: Optional[float] = None
    ) -> FunctionInfo:
        """
        Retrieve a function by its full name asynchronously.

        Args:
            function_name: The full name of the function to retrieve.
                It should be in the format of "catalog.schema.function_name".
            timeout: The timeout in seconds.

        Returns:
            The FunctionInfo object representing the function.
        """

        try:
            return await self.uc.functions_client.get_function(
                function_name, _request_timeout=timeout
            )
        except NotFoundException as e:
            _logger.warning(
                f"Failed to retrieve function {function_name} from Unity Catalog, the function may not exist. "
                f"Exception: {e}"
            )

    @override
    @syncify_method
    def get_function(self, function_name: str, timeout: Optional[float] = None) -> FunctionInfo:
        """
        Retrieve a function by its full name.

        This is a synchronous version of `get_function_async`.

        Args:
            function_name: The full name of the function to retrieve.
                It should be in the format of "catalog.schema.function_name".
            timeout: The timeout in seconds.

        Returns:
            The FunctionInfo object representing the function.
        """

        pass

    async def list_functions_async(
        self,
        catalog: str,
        schema: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> PagedList[FunctionInfo]:
        """
        List functions in a given catalog and schema asynchronously.

        Args:
            catalog: The catalog name.
            schema: The schema name.
            max_results: The maximum number of results to return.
            page_token: The token for pagination.
            timeout: The timeout in seconds.

        Returns:
            A PagedList of FunctionInfo objects.
        """
        resp = await self.uc.functions_client.list_functions(
            catalog_name=catalog,
            schema_name=schema,
            max_results=max_results,
            page_token=page_token,
            _request_timeout=timeout,
        )

        return PagedList(resp.functions, resp.next_page_token)

    @override
    @syncify_method
    def list_functions(
        self,
        catalog: str,
        schema: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> PagedList[FunctionInfo]:
        """
        List functions in a given catalog and schema.

        This is a synchronous version of `list_functions_async`.

        Args:
            catalog: The catalog name.
            schema: The schema name.
            max_results: The maximum number of results to return.
            page_token: The token for pagination.
            timeout: The timeout in seconds.

        Returns:
            A PagedList of FunctionInfo objects.
        """
        pass

    @override
    def _validate_param_type(self, value: Any, param_info: FunctionParameterInfo) -> None:
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
            python_function = dynamically_construct_python_function(function_info)
            exec(python_function, self.func_cache)
            try:
                func = self.func_cache[function_info.name]

                result = func(**parameters)

                self.func_cache[function_info.name] = lru_cache()(func)

                return FunctionExecutionResult(format="SCALAR", value=str(result))
            except Exception as e:
                return FunctionExecutionResult(error=str(e))

    async def delete_function_async(
        self,
        function_name: str,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Delete a function by its full name asynchronously.

        Args:
            function_name: The full name of the function to delete.
                It should be in the format of "catalog.schema.function_name".
            timeout: The timeout in seconds.

        Returns:
            None
        """

        await self.uc.functions_client.delete_function(function_name, _request_timeout=timeout)

    @override
    @syncify_method
    def delete_function(self, function_name: str, timeout: Optional[float] = None) -> None:
        """
        Delete a function by its full name.

        This is a synchronous version of `delete_function_async`.

        Args:
            function_name: The full name of the function to delete.
                It should be in the format of "catalog.schema.function_name".
            timeout: The timeout in seconds.

        Returns:
            None
        """

        pass

    @override
    def to_dict(self) -> Dict[str, Any]:
        elements = ["uc"]
        return {k: getattr(self, k) for k in elements if getattr(self, k) is not None}


def dynamically_construct_python_function(function_info: FunctionInfo) -> str:
    """
    Construct a Python function from the given FunctionInfo.

    Args:
        function_info: The FunctionInfo object containing the function metadata.

    Returns:
        The re-constructed function definition.
    """

    param_names = []
    if function_info.input_params and function_info.input_params.parameters:
        param_names = [param.name for param in function_info.input_params.parameters]
    function_head = f"{function_info.name}({', '.join(param_names)})"
    func_def = f"def {function_head}:\n"
    if function_info.routine_body == "EXTERNAL":
        for line in function_info.routine_definition.split("\n"):
            func_def += f"    {line}\n"
    else:
        raise NotImplementedError(f"routine_body {function_info.routine_body} not supported")

    return func_def


def validate_input_parameter(
    parameter: Union[FunctionParameterInfo, Dict[str, str]],
) -> FunctionParameterInfo:
    """
    Validate the input parameter and convert it to FunctionParameterInfo.

    Args:
        parameter: The input parameter to validate. If it is a dict, it will be converted to FunctionParameterInfo.

    Returns:
        The validated FunctionParameterInfo.
    """

    if isinstance(parameter, dict):
        parameter = FunctionParameterInfo(**parameter)
    elif not isinstance(parameter, FunctionParameterInfo):
        raise TypeError(
            f"Input parameter should be either a dict or an instance of "
            f"FunctionParameterInfo, but got {type(parameter)}"
        )

    required_fields = ["name", "type_name", "type_text", "type_json"]

    if missing_fields := [
        field for field in required_fields if getattr(parameter, field, None) is None
    ]:
        raise ValueError(
            f"Missing required fields in input parameter '{parameter}': {missing_fields}."
        )

    if isinstance(type_name := parameter.type_name, Enum):
        type_name = type_name.value
    elif not isinstance(type_name, str):
        raise ValueError(
            f"Invalid type_name '{type_name}' in input parameter '{parameter.name}'. Must be a string or Enum instance."
        )

    complex_type_names = ["ARRAY", "MAP", "STRUCT", "DECIMAL", "INTERVAL"]
    for complex_type_name in complex_type_names:
        if type_name.startswith(complex_type_name):
            parameter.type_name = complex_type_name
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
