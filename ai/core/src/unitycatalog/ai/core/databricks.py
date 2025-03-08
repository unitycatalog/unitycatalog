import base64
import functools
import json
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from io import StringIO
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from typing_extensions import override

from unitycatalog.ai.core.base import BaseFunctionClient, FunctionExecutionResult
from unitycatalog.ai.core.envs.databricks_env_vars import (
    UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT,
    UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS,
)
from unitycatalog.ai.core.executor.local_subprocess import run_in_sandbox_subprocess
from unitycatalog.ai.core.paged_list import PagedList
from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.callable_utils import (
    dynamically_construct_python_function,
    generate_sql_function_body,
    generate_wrapped_sql_function_body,
    get_callable_definition,
)
from unitycatalog.ai.core.utils.execution_utils import load_function_from_string
from unitycatalog.ai.core.utils.function_processing_utils import process_function_parameter_defaults
from unitycatalog.ai.core.utils.type_utils import (
    column_type_to_python_type,
    convert_timedelta_to_interval_str,
    is_time_type,
)
from unitycatalog.ai.core.utils.validation_utils import (
    FullFunctionName,
    check_function_info,
    validate_param,
)

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        FunctionInfo,
        FunctionParameterInfo,
    )
    from databricks.sdk.service.sql import StatementState

DATABRICKS_CONNECT_SUPPORTED_VERSION = "15.1.0"
DATABRICKS_CONNECT_IMPORT_ERROR_MESSAGE = (
    "Could not import databricks-connect python package. "
    "To interact with UC functions using serverless compute, install the package with "
    f"`pip install databricks-connect>={DATABRICKS_CONNECT_SUPPORTED_VERSION}`. "
    "Please note this requires python>=3.10."
)
DATABRICKS_CONNECT_VERSION_NOT_SUPPORTED_ERROR_MESSAGE = (
    "Serverless is not supported by the "
    "current databricks-connect version, install with "
    f"`pip install databricks-connect>={DATABRICKS_CONNECT_SUPPORTED_VERSION}` "
    "to use serverless compute in Databricks. Please note this requires python>=3.10."
)
SESSION_RETRY_BASE_DELAY = 1
SESSION_RETRY_MAX_DELAY = 4
SESSION_EXPIRED_MESSAGE = "session_id is no longer usable"
SESSION_CHANGED_MESSAGE = "The existing Spark server driver has restarted."
WAREHOUSE_DEFINED_NOT_SUPPORTED_MESSAGE = (
    "The argument `warehouse_id` was specified, which is no longer supported with "
    "the `DatabricksFunctionClient`. Please omit this argument as it is no longer used. "
    "This API only functions with a serverless compute resource."
    "serverless compute resource for interfacing with Unity Catalog."
    "Visit https://docs.unitycatalog.io/ai/client/#databricks-function-client for more details."
)


_logger = logging.getLogger(__name__)


class ExecutionMode(str, Enum):
    SERVERLESS = "serverless"
    LOCAL = "local"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def validate(cls, value: str) -> "ExecutionMode":
        try:
            if value == cls.LOCAL.value:
                _logger.warning(
                    "You are running in 'local' execution mode, which is intended only for development and debugging. "
                    "For production, please switch to 'serverless' execution mode. Before deploying, create a client "
                    "using 'serverless' mode to validate your code's behavior and ensure full compatibility."
                )
            return cls(value)
        except ValueError as e:
            raise ValueError(
                f"Execution mode '{value}' is not valid. "
                f"Allowed values are: {', '.join(mode.value for mode in cls)}"
            ) from e


class SessionExpirationException(Exception):
    """Exception raised when a session expiration error is detected."""

    pass


def get_default_databricks_workspace_client(profile=None) -> "WorkspaceClient":
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as e:
        raise ImportError(
            "Could not import databricks-sdk python package. "
            "If you want to use databricks backend then "
            "please install it with `pip install databricks-sdk`."
        ) from e
    return WorkspaceClient(profile=profile)


def _validate_databricks_connect_available() -> bool:
    try:
        from databricks.connect.session import DatabricksSession  # noqa: F401

        if not hasattr(DatabricksSession.builder, "serverless"):
            raise Exception(DATABRICKS_CONNECT_VERSION_NOT_SUPPORTED_ERROR_MESSAGE)
    except ImportError as e:
        raise Exception(DATABRICKS_CONNECT_IMPORT_ERROR_MESSAGE) from e


def _is_in_databricks_notebook_environment() -> bool:
    try:
        from dbruntime.databricks_repl_context import get_context

        return get_context().isInNotebook
    except Exception:
        return False


def _warn_if_workspace_provided(**kwargs):
    if "warehouse_id" in kwargs:
        _logger.warning(WAREHOUSE_DEFINED_NOT_SUPPORTED_MESSAGE)


def extract_function_name(sql_body: str) -> str:
    """
    Extract function name from the sql body.
    CREATE FUNCTION syntax reference: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax
    """
    # NOTE: catalog/schema/function names follow guidance here:
    # https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html#catalog-name
    pattern = re.compile(
        r"""
        CREATE\s+(?:OR\s+REPLACE\s+)?      # Match 'CREATE OR REPLACE' or just 'CREATE'
        (?:TEMPORARY\s+)?                  # Match optional 'TEMPORARY'
        FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?  # Match 'FUNCTION' and optional 'IF NOT EXISTS'
        (?P<name>[^ /.]+\.[^ /.]+\.[^ /.]+)          # Capture the function name (including schema if present)
        \s*\(                              # Match opening parenthesis after function name
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    match = pattern.search(sql_body)
    if match:
        result = match.group("name")
        full_function_name = FullFunctionName.validate_full_function_name(result)
        # backticks are only required in SQL, not in python APIs
        return str(full_function_name)
    raise ValueError(
        f"Could not extract function name from the sql body: {sql_body}.\nPlease "
        "make sure the sql body follows the syntax of CREATE FUNCTION "
        "statement in Databricks: "
        "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax."
    )


def retry_on_session_expiration(func):
    """
    Decorator to retry a method upon session expiration errors with exponential backoff.
    """
    max_attempts = UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get()

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                result = func(self, *args, **kwargs)
                if (
                    isinstance(result, FunctionExecutionResult)
                    and result.error
                    and any(
                        msg in result.error
                        for msg in (SESSION_EXPIRED_MESSAGE, SESSION_CHANGED_MESSAGE)
                    )
                ):
                    raise SessionExpirationException(result.error)
                return result
            except SessionExpirationException as e:
                if not self._is_default_client:
                    refresh_message = (
                        f"Failed to execute {func.__name__} due to session expiration. "
                        "Unable to automatically refresh session when using a custom client. "
                        "Recreate the DatabricksFunctionClient with a new client to recreate "
                        "the custom client session."
                    )
                    raise RuntimeError(refresh_message) from e

                if attempt < max_attempts:
                    delay = min(
                        SESSION_RETRY_BASE_DELAY * (2 ** (attempt - 1)), SESSION_RETRY_MAX_DELAY
                    )
                    _logger.warning(
                        f"Session expired. Retrying attempt {attempt} of {max_attempts}. "
                        f"Refreshing session and retrying after {delay} seconds..."
                    )
                    self.refresh_client_and_session()
                    time.sleep(delay)
                    continue
                else:
                    refresh_failure_message = (
                        f"Failed to execute {func.__name__} after {max_attempts} attempts due to session expiration. "
                        "Generate a new session_id by detaching and reattaching your compute."
                    )
                    raise RuntimeError(refresh_failure_message) from e

    return wrapper


class DatabricksFunctionClient(BaseFunctionClient):
    """
    Databricks UC function calling client
    """

    def __init__(
        self,
        client: Optional["WorkspaceClient"] = None,
        *,
        profile: Optional[str] = None,
        execution_mode: str = "serverless",
        **kwargs: Any,
    ) -> None:
        """
        Databricks UC function calling client.

        Args:
            client: The databricks workspace client. If it's None, a default databricks workspace client
                is generated based on the configuration. Defaults to None.
            profile: The configuration profile to use for databricks connect. Defaults to None.
            execution_mode: The execution mode of the client. Defaults to "serverless".
        """
        _warn_if_workspace_provided(**kwargs)
        self.client = client or get_default_databricks_workspace_client(profile=profile)
        self.profile = profile
        self.execution_mode = ExecutionMode.validate(execution_mode)
        self.spark = None
        self.set_spark_session()
        self._is_default_client = client is None
        super().__init__()

    def _is_spark_session_active(self):
        if self.spark is None:
            return False
        if hasattr(self.spark, "is_stopped"):
            return not self.spark.is_stopped
        return self.spark.getActiveSession() is not None

    def set_spark_session(self):
        """
        Initialize the spark session with serverless compute if not already active.
        """
        if not self._is_spark_session_active():
            self.initialize_spark_session()

    def stop_spark_session(self):
        if self._is_spark_session_active():
            self.spark.stop()

    def initialize_spark_session(self):
        """
        Initialize the spark session with serverless compute.
        This method is called when the spark session is not active.
        """
        _validate_databricks_connect_available()

        from databricks.connect.session import DatabricksSession as SparkSession

        if self.profile:
            builder = SparkSession.builder.profile(self.profile).serverless(True)
        elif self.client is not None:
            config = self.client.config
            config.as_dict().pop("cluster_id", None)
            config.serverless_compute_id = "auto"  # Setting Serverless to true by adding "auto"
            builder = SparkSession.builder.sdkConfig(config)
        else:
            builder = SparkSession.builder.serverless(True)
        self.spark = builder.getOrCreate()

    def refresh_client_and_session(self):
        """
        Refreshes the databricks client and spark session if the session_id has been invalidated due to expiration of temporary credentials.
        If the client is running within an interactive Databricks notebook environment, the spark session is not terminated.
        """

        _logger.info("Refreshing Databricks client and Spark session due to session expiration.")
        self.client = get_default_databricks_workspace_client(profile=self.profile)
        if not _is_in_databricks_notebook_environment():
            self.stop_spark_session()
        self.initialize_spark_session()

    @retry_on_session_expiration
    @override
    def create_function(
        self,
        *,
        sql_function_body: Optional[str] = None,
    ) -> "FunctionInfo":
        """
        Create a UC function with the given sql body or function info.

        Note: `databricks-connect` is required to use this function, make sure its version is 15.1.0 or above to use
            serverless compute.

        Args:
            sql_function_body: The sql body of the function. Defaults to None.
                It should follow the syntax of CREATE FUNCTION statement in Databricks.
                Ref: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax

        Returns:
            FunctionInfo: The created function info.
        """
        if sql_function_body:
            self.set_spark_session()
            # TODO: add timeout
            self.spark.sql(sql_function_body)
            created_function_info = self.get_function(extract_function_name(sql_function_body))
            check_function_info(created_function_info)
            return created_function_info
        # TODO: support creating from function_info after CreateFunction bug is fixed in databricks-sdk
        # return self.client.functions.create(function_info)
        raise ValueError("sql_function_body must be provided.")

    @retry_on_session_expiration
    @override
    def create_python_function(
        self,
        *,
        func: Callable[..., Any],
        catalog: str,
        schema: str,
        replace: bool = False,
        dependencies: Optional[list[str]] = None,
        environment_version: str = "None",
    ) -> "FunctionInfo":
        """
        Create a Unity Catalog (UC) function directly from a Python function.

        This API allows you to convert a Python function into a Unity Catalog User-Defined Function (UDF).
        It automates the creation of UC functions while ensuring that the Python function meets certain
        criteria and adheres to best practices.

        **Requirements:**

        1. **Type Annotations**:
            - The Python function must use argument and return type annotations. These annotations are used
            to generate the SQL signature of the UC function.
            - Supported Python types and their corresponding UC types are as follows:

            | Python Type          | Unity Catalog Type       |
            |----------------------|--------------------------|
            | `int`                | `LONG`                |
            | `float`              | `DOUBLE`                 |
            | `str`                | `STRING`                 |
            | `bool`               | `BOOLEAN`                |
            | `Decimal`            | `DECIMAL`                |
            | `datetime.date`      | `DATE`                   |
            | `datetime.timedelta` | `INTERVAL DAY TO SECOND` |
            | `datetime.datetime`  | `TIMESTAMP`              |
            | `list`               | `ARRAY`                  |
            | `tuple`              | `ARRAY`                  |
            | `dict`               | `MAP`                    |
            | `bytes`              | `BINARY`                 |

            - **Example of a valid function**:
            ```python
            def my_function(a: int, b: str) -> float:
                return a + len(b)
            ```

            - **Invalid function (missing type annotations)**:
            ```python
            def my_function(a, b):
                return a + len(b)
            ```
            Attempting to create a UC function from a function without type hints will raise an error, as the
            system relies on type hints to generate the UC function's signature.

            - For container types like `list`, `tuple` and `dict`, the inner types **must be specified** and must be
            uniform (Union types are not permitted). For example:

            ```python
            def my_function(a: List[int], b: Dict[str, float]) -> List[str]:
                return [str(x) for x in a]
            ```

            - var args and kwargs are not supported. All arguments must be explicitly defined in the function signature.

        2. **Google Docstring Guidelines**:
            - It is required to include detailed Python docstrings in your function to provide additional context.
            The docstrings will be used to auto-generate parameter descriptions and a function-level comment.

            - A **function description** must be provided at the beginning of the docstring (within the triple quotes)
            to describe the function's purpose. This description will be used as the function-level comment in the UC function.
            The description **must** be included in the first portion of the docstring prior to any argument descriptions.

            - **Parameter descriptions** are optional but recommended. If provided, they should be included in the
            Google-style docstring. The parameter descriptions will be used to auto-generate detailed descriptions for
            each parameter in the UC function. The additional context provided by these argument descriptions can be
            useful for agent applications to understand context of the arguments and their purpose.

            - Only **Google-style docstrings** are supported for this auto-generation. For example:
            ```python
            def my_function(a: int, b: str) -> float:
                \"\"\"
                Adds the length of a string to an integer.

                Args:
                    a (int): The integer to add to.
                    b (str): The string whose length will be added.

                Returns:
                    float: The sum of the integer and the string length.
                \"\"\"
                return a + len(b)
            ```
            - If docstrings do not conform to Google-style for specifying arguments descriptions, parameter descriptions
             will default to `"Parameter <name>"`, and no further information will be provided in the function comment
             for the given parameter.

            For examples of Google docstring guidelines, see
            [this link](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)

        3. **External Dependencies**:
            - Unity Catalog UDFs are limited to Python standard libraries and Databricks-provided libraries. If your
            function relies on unsupported external dependencies, the created UC function may fail at runtime.
            - It is strongly recommended to test the created function by executing it before integrating it into
            GenAI or other tools.

        **Function Metadata**:
        - Docstrings (if provided and Google-style) will automatically be included as detailed descriptions for
        function parameters as well as for the function itself, enhancing the discoverability of the utility of your
        UC function.

        **Example**:
        ```python
        def example_function(x: int, y: int) -> float:
            \"\"\"
            Multiplies an integer by the length of a string.

            Args:
                x (int): The number to be multiplied.
                y (int): A string whose length will be used for multiplication.

            Returns:
                float: The product of the integer and the string length.
            \"\"\"
            return x * len(y)

        client.create_python_function(
            func=example_function,
            catalog="my_catalog",
            schema="my_schema"
        )
        ```

        **Overwriting a function**:
        - If a function with the same name already exists in the specified catalog and schema, the function will not be
        created by default. To overwrite the existing function, set the `replace` parameter to `True`.

        Args:
            func: The Python function to convert into a UDF.
            catalog: The catalog name in which to create the function.
            schema: The schema name in which to create the function.
            replace: Whether to replace the function if it already exists. Defaults to False.
            dependencies: A list of external dependencies required by the function. Defaults to an empty list. Note that the
                `dependencies` parameter is not supported in all runtimes. Ensure that you are using a runtime that supports environment
                and dependency declaration prior to creating a function that defines dependencies.
                Standard PyPI package declarations are supported (i.e., `requests>=2.25.1`).
            environment_version: The version of the environment in which the function will be executed. Defaults to 'None'. Note
                that the `environment_version` parameter is not supported in all runtimes. Ensure that you are using a runtime that
                supports environment and dependency declaration prior to creating a function that declares an environment verison.

        Returns:
            FunctionInfo: Metadata about the created function, including its name and signature.
        """

        if not callable(func):
            raise ValueError("The provided function is not callable.")

        sql_function_body = generate_sql_function_body(
            func, catalog, schema, replace, dependencies, environment_version
        )

        try:
            return self.create_function(sql_function_body=sql_function_body)
        except Exception as e:
            if "Parameter default value is not supported" in str(e):
                # this is a known issue with external python functions in Unity Catalog. Defining a SQL body statement
                # can be used as a workaround for this issue.
                raise ValueError(
                    "Default parameters are not permitted with the create_python_function API. "
                    "Specify a SQL body statement for defaults and use the create_function API "
                    "to define functions with default values."
                ) from e
            raise e

    @retry_on_session_expiration
    @override
    def create_wrapped_function(
        self,
        *,
        primary_func: Callable[..., Any],
        functions: list[Callable[..., Any]],
        catalog: str,
        schema: str,
        replace=False,
        dependencies: Optional[list[str]] = None,
        environment_version: str = "None",
    ) -> "FunctionInfo":
        """
        Create a wrapped function comprised of a `primary_func` function and in-lined wrapped `functions` within the `primary_func` body.

        Note: `databricks-connect` is required to use this function, make sure its version is 15.1.0 or above to use
            serverless compute.

        Args:
            primary_func: The primary function to be wrapped.
            functions: A list of functions to be wrapped inline within the body of `primary_func`.
            catalog: The catalog name.
            schema: The schema name.
            replace: Whether to replace the function if it already exists. Defaults to False.
            dependencies: A list of external dependencies required by the function. Defaults to an empty list. Note that the
                `dependencies` parameter is not supported in all runtimes. Ensure that you are using a runtime that supports environment
                and dependency declaration prior to creating a function that defines dependencies.
                Standard PyPI package declarations are supported (i.e., `requests>=2.25.1`).
            environment_version: The version of the environment in which the function will be executed. Defaults to 'None'. Note
                that the `environment_version` parameter is not supported in all runtimes. Ensure that you are using a runtime that
                supports environment and dependency declaration prior to creating a function that declares an environment verison.

        Returns:
            FunctionInfo: Metadata about the created function, including its name and signature.
        """
        if not callable(primary_func):
            raise ValueError("The provided primary function is not callable.")

        sql_function_body = generate_wrapped_sql_function_body(
            primary_func=primary_func,
            functions=functions,
            catalog=catalog,
            schema=schema,
            replace=replace,
            dependencies=dependencies,
            environment_version=environment_version,
        )

        return self.create_function(sql_function_body=sql_function_body)

    @override
    def get_function(self, function_name: str, **kwargs: Any) -> "FunctionInfo":
        """
        Get a function by its name.

        Args:
            function_name: The name of the function to get.
            kwargs: additional key-value pairs to include when getting the function.
            Allowed keys for retrieving functions are:
            - include_browse: bool (default to None)
                Whether to include functions in the response for which the principal can only
                access selective metadata for.

        Note:
            The function name shouldn't be *, to get all functions in a catalog and schema,
            please use list_functions API instead.

        Returns:
            FunctionInfo: The function info.
        """
        from databricks.sdk.errors.platform import PermissionDenied

        full_func_name = FullFunctionName.validate_full_function_name(function_name)
        if "*" in full_func_name.function:
            raise ValueError(
                "function name cannot include *, to get all functions in a catalog and schema, "
                "please use list_functions API instead."
            )
        try:
            return self.client.functions.get(function_name)
        except PermissionDenied as e:
            raise PermissionError(f"Permission denied: {e}") from e

    @override
    def list_functions(
        self,
        catalog: str,
        schema: str,
        max_results: Optional[int] = None,
        page_token: Optional[str] = None,
        include_browse: Optional[bool] = None,
    ) -> PagedList["FunctionInfo"]:
        """
        List functions in a catalog and schema.

        Args:
            catalog: The catalog name.
            schema: The schema name.
            max_results: The maximum number of functions to return. Defaults to None.
            page_token: The token for the next page. Defaults to None.
            include_browse: Whether to include functions in the response for which the
            principal can only access selective metadata for. Defaults to None.

        Returns:
            PageList[FunctionInfo]: The paginated list of function infos.
        """
        from databricks.sdk.service.catalog import FunctionInfo

        # do not reuse self.client.functions.list because the list API in databricks-sdk
        # doesn't work for max_results and page_token
        query = {}
        if catalog is not None:
            query["catalog_name"] = catalog
        if max_results is not None:
            query["max_results"] = max_results
        if page_token is not None:
            query["page_token"] = page_token
        if schema is not None:
            query["schema_name"] = schema
        if include_browse is not None:
            query["include_browse"] = include_browse
        headers = {
            "Accept": "application/json",
        }

        function_infos = []
        json = self.client.functions._api.do(
            "GET", "/api/2.1/unity-catalog/functions", query=query, headers=headers
        )
        if "functions" in json:
            function_infos = [FunctionInfo.from_dict(v) for v in json["functions"]]
        token = json.get("next_page_token")
        return PagedList(function_infos, token)

    @override
    def _validate_param_type(self, value: Any, param_info: "FunctionParameterInfo") -> None:
        value_python_type = column_type_to_python_type(param_info.type_name.value)
        if value_python_type is Variant:
            Variant.validate(value)
            return
        if not isinstance(value, value_python_type):
            raise ValueError(
                f"Parameter {param_info.name} should be of type {param_info.type_name.value} "
                f"(corresponding python type {value_python_type}), but got {type(value)}"
            )
        validate_param(value, param_info.type_name.value, param_info.type_text)

    @override
    def execute_function(
        self, function_name: str, parameters: Optional[Dict[str, Any]] = None, **kwargs: Any
    ) -> FunctionExecutionResult:
        """
        Execute a UC function by name with the given parameters.

        Args:
            function_name: The name of the function to execute.
            parameters: The parameters to pass to the function. Defaults to None.
            kwargs: additional key-value pairs to include when executing the function.
                Allowed keys for retrieving functions are:
                - include_browse: bool (default to False)
                    Whether to include functions in the response for which the principal can only access selective
                    metadata for.
                Allowed keys for executing functions are:
                - wait_timeout: str (default to `30s`)
                    The time in seconds the call will wait for the statement's result set as `Ns`, where `N` can be set
                    to 0 or to a value between 5 and 50.

                    When set to `0s`, the statement will execute in asynchronous mode and the call will not wait for the
                    execution to finish. In this case, the call returns directly with `PENDING` state and a statement ID
                    which can be used for polling with :method:statementexecution/getStatement.

                    When set between 5 and 50 seconds, the call will behave synchronously up to this timeout and wait
                    for the statement execution to finish. If the execution finishes within this time, the call returns
                    immediately with a manifest and result data (or a `FAILED` state in case of an execution error). If
                    the statement takes longer to execute, `on_wait_timeout` determines what should happen after the
                    timeout is reached.
                - row_limit: int (default to 100)
                    Applies the given row limit to the statement's result set, but unlike the `LIMIT` clause in SQL, it
                    also sets the `truncated` field in the response to indicate whether the result was trimmed due to
                    the limit or not.
                - byte_limit: int (default to 1048576 = 1MB)
                    Applies the given byte limit to the statement's result size. Byte counts are based on internal data
                    representations and might not match the final size in the requested `format`. If the result was
                    truncated due to the byte limit, then `truncated` in the response is set to `true`. When using
                    `EXTERNAL_LINKS` disposition, a default `byte_limit` of 100 GiB is applied if `byte_limit` is not
                    explcitly set.

        Returns:
            FunctionExecutionResult: The result of executing the function.
        """
        return super().execute_function(function_name, parameters, **kwargs)

    @override
    def _execute_uc_function(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any], **kwargs: Any
    ) -> Any:
        check_function_info(function_info)
        if self.execution_mode == ExecutionMode.SERVERLESS:
            return self._execute_uc_functions_with_serverless(function_info, parameters)
        elif self.execution_mode == ExecutionMode.LOCAL:
            return self._execute_uc_functions_with_local(function_info, parameters)
        else:
            raise NotImplementedError(
                f"Execution mode {self.execution_mode} is not supported for function execution."
            )

    @retry_on_session_expiration
    def _execute_uc_functions_with_serverless(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any]
    ) -> FunctionExecutionResult:
        _logger.info("Using databricks connect to execute functions with serverless compute.")
        self.set_spark_session()

        parameters = process_function_parameter_defaults(function_info, parameters)

        sql_command = get_execute_function_sql_command(function_info, parameters)
        try:
            result = self.spark.sql(sqlQuery=sql_command.sql_query, args=sql_command.args or None)
            if is_scalar(function_info):
                return FunctionExecutionResult(format="SCALAR", value=str(result.collect()[0][0]))
            else:
                row_limit = UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT.get()
                truncated = result.count() > row_limit
                pdf = result.limit(row_limit).toPandas()
                csv_buffer = StringIO()
                pdf.to_csv(csv_buffer, index=False)
                return FunctionExecutionResult(
                    format="CSV", value=csv_buffer.getvalue(), truncated=truncated
                )
        except Exception as e:
            sql_command_msg = (
                f"spark.sql({sql_command.sql_query}" + f", args={sql_command.args})"
                if sql_command.args
                else ")"
            )
            error = f"Failed to execute function with command `{sql_command_msg}`\nError: {e}"
            return FunctionExecutionResult(error=error)

    def _execute_uc_functions_with_local(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any]
    ) -> FunctionExecutionResult:
        if not is_scalar(function_info):
            raise ValueError(
                "Local sandbox execution is only supported for scalar Python functions. "
                "Use 'serverless' execution mode for table functions."
            )
        _logger.info("Using local sandbox to execute functions.")

        parameters = process_function_parameter_defaults(function_info, parameters)
        python_def = get_callable_definition(function_info)
        succeeded, result = run_in_sandbox_subprocess(python_def, parameters)
        if not succeeded:
            return FunctionExecutionResult(error=result)
        return FunctionExecutionResult(format="SCALAR", value=result)

    @override
    def delete_function(
        self,
        function_name: str,
        force: Optional[bool] = None,
    ) -> None:
        """
        Delete a function by its full name.

        Args:
            function_name: The full name of the function to delete.
                It should be in the format of "catalog.schema.function_name".
            force: Force deletion even if the function is not empty. This
                parameter is used by underlying databricks workspace client
                when deleting a function. If it is None then the parameter
                is not included in the request. Defaults to None.
        """
        self.client.functions.delete(function_name, force=force)

    @override
    def to_dict(self):
        return {
            # TODO: workspaceClient related config
            "profile": self.profile,
        }

    @classmethod
    def from_dict(cls, config: Dict[str, Any]):
        accept_keys = ["profile"]
        return cls(**{k: v for k, v in config.items() if k in accept_keys})

    @override
    def get_function_source(self, function_name: str) -> str:
        """
        Returns the Python callable definition as a string for an EXTERNAL Python function that
        is stored within Unity Catalog. This function can only parse and extract the full callable
        definition for Python functions and cannot be used on SQL or TABLE functions.

        Args:
            function_name: The name of the function to retrieve the Python callable definition for.

        Returns:
            str: The Python callable definition as a string.
        """

        function_info = self.get_function(function_name)
        if function_info.routine_body.value != "EXTERNAL":
            raise ValueError(
                f"Function {function_name} is not an EXTERNAL Python function and cannot be retrieved."
            )
        return dynamically_construct_python_function(function_info)

    @override
    def get_function_as_callable(
        self, function_name: str, register_function: bool = True, namespace: dict[str, Any] = None
    ) -> Callable[..., Any]:
        """
        Returns the Python callable for an EXTERNAL Python function that is stored within Unity Catalog.
        This function can only parse and extract the full callable definition for Python functions and
        cannot be used on SQL or TABLE functions.

        Args:
            function_name: The name of the function to retrieve the Python callable for.
            register_function: Whether to register the function in the namespace. Defaults to True.
            namespace: The namespace to register the function in. Defaults to None (global)

        Returns:
            Callable[..., Any]: The Python callable for the function.
        """
        source = self.get_function_source(function_name)
        return load_function_from_string(
            func_def_str=source, register_function=register_function, namespace=namespace
        )


def is_scalar(function: "FunctionInfo") -> bool:
    from databricks.sdk.service.catalog import ColumnTypeName

    return function.data_type != ColumnTypeName.TABLE_TYPE


def job_pending(state: "StatementState") -> bool:
    from databricks.sdk.service.sql import StatementState

    return state in (StatementState.PENDING, StatementState.RUNNING)


@dataclass
class SparkSqlCommand:
    sql_query: str
    args: dict[str, Any]


def get_execute_function_sql_command(
    function: "FunctionInfo", parameters: Dict[str, Any]
) -> SparkSqlCommand:
    from databricks.sdk.service.catalog import ColumnTypeName

    sql_query = ""
    if is_scalar(function):
        sql_query += f"SELECT `{function.catalog_name}`.`{function.schema_name}`.`{function.name}`("
    else:
        sql_query += (
            f"SELECT * FROM `{function.catalog_name}`.`{function.schema_name}`.`{function.name}`("
        )

    params_dict: dict[str, Any] = {}
    if parameters and function.input_params and function.input_params.parameters:
        args: List[str] = []
        use_named_args = False

        for param_info in function.input_params.parameters:
            if param_info.name not in parameters:
                use_named_args = True
            else:
                arg_clause = ""
                if use_named_args:
                    arg_clause += f"{param_info.name} => "
                param_value = parameters[param_info.name]
                if param_info.type_name in (
                    ColumnTypeName.ARRAY,
                    ColumnTypeName.MAP,
                    ColumnTypeName.STRUCT,
                ):
                    json_value_str = json.dumps(param_value)
                    arg_clause += f"from_json('{json_value_str}', '{param_info.type_text}')"
                elif param_info.type_name == ColumnTypeName.VARIANT:
                    json_value_str = json.dumps(param_value)
                    arg_clause += f"parse_json('{json_value_str}')"
                elif param_info.type_name == ColumnTypeName.BINARY:
                    if isinstance(param_value, bytes):
                        param_value = base64.b64encode(param_value).decode("utf-8")
                    # Use ubbase64 to restore binary values.
                    arg_clause += f"unbase64('{param_value}')"
                elif is_time_type(param_info.type_name.value):
                    date_str = (
                        param_value if isinstance(param_value, str) else param_value.isoformat()
                    )
                    arg_clause += f"'{date_str}'"
                elif param_info.type_name == ColumnTypeName.INTERVAL:
                    interval_str = (
                        convert_timedelta_to_interval_str(param_value)
                        if not isinstance(param_value, str)
                        else param_value
                    )
                    arg_clause += interval_str
                else:
                    if param_info.type_name == ColumnTypeName.DECIMAL and isinstance(
                        param_value, Decimal
                    ):
                        param_value = float(param_value)
                    arg_clause += f":{param_info.name}"
                    params_dict[param_info.name] = param_value
                args.append(arg_clause)
        sql_query += ",".join(args)
    sql_query += ")"
    return SparkSqlCommand(sql_query=sql_query, args=params_dict)
