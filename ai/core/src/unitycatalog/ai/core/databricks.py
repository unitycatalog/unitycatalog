import base64
import functools
import json
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from typing_extensions import override

from unitycatalog.ai.core.base import BaseFunctionClient, FunctionExecutionResult
from unitycatalog.ai.core.envs.databricks_env_vars import (
    UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT,
    UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT,
    UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT,
)
from unitycatalog.ai.core.paged_list import PagedList
from unitycatalog.ai.core.utils.callable_utils import generate_sql_function_body
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
    from databricks.sdk.service.sql import StatementParameterListItem, StatementState

DATABRICKS_CONNECT_SUPPORTED_VERSION = "15.1.0"
DATABRICKS_CONNECT_IMPORT_ERROR_MESSAGE = (
    "Could not import databricks-connect python package. "
    "To interact with UC functions using serverless compute, install the package with "
    f"`pip install databricks-connect=={DATABRICKS_CONNECT_SUPPORTED_VERSION}`. "
    "Please note this requires python>=3.10."
)
DATABRICKS_CONNECT_VERSION_NOT_SUPPORTED_ERROR_MESSAGE = (
    "Serverless is not supported by the "
    "current databricks-connect version, install with "
    f"`pip install databricks-connect=={DATABRICKS_CONNECT_SUPPORTED_VERSION}` "
    "to use serverless compute in Databricks. Please note this requires python>=3.10."
)
SESSION_RETRY_BASE_DELAY = 1
SESSION_RETRY_MAX_DELAY = 32
SESSION_EXCEPTION_MESSAGE = "session_id is no longer usable"

_logger = logging.getLogger(__name__)


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


def _try_get_spark_session_in_dbr() -> Any:
    try:
        # if in Databricks, fetch the current active session
        from databricks.sdk.runtime import spark
        from pyspark.sql.connect.session import SparkSession

        if not isinstance(spark, SparkSession):
            _logger.warning(
                "Current SparkSession in the active environment is not a "
                "pyspark.sql.connect.session.SparkSession instance. Classic runtime does not support "
                "all functionalities of the unitycatalog-ai framework. To use the full "
                "capabilities of unitycatalog-ai, execute your code using a client that is attached to "
                "a Serverless runtime cluster. To learn more about serverless, see the guide at: "
                "https://docs.databricks.com/en/compute/serverless/index.html#connect-to-serverless-compute "
                "for more details."
            )
        return spark
    except Exception:
        return


def _is_in_databricks_notebook_environment() -> bool:
    try:
        from dbruntime.databricks_repl_context import get_context

        return get_context().isInNotebook
    except Exception:
        return False


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
    max_attempts = int(UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS.get())

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                result = func(self, *args, **kwargs)
                # for non-session related error in the result, we should directly return the result
                if (
                    isinstance(result, FunctionExecutionResult)
                    and result.error
                    and SESSION_EXCEPTION_MESSAGE in result.error
                ):
                    raise Exception(result.error)
                return result
            except Exception as e:
                error_message = str(e)
                if SESSION_EXCEPTION_MESSAGE in error_message:
                    if not self._is_default_client:
                        refresh_message = f"Failed to execute {func.__name__} due to session expiration. Unable to automatically refresh session when using a custom client."
                        raise RuntimeError(refresh_message) from e

                    if attempt < max_attempts:
                        delay = min(
                            SESSION_RETRY_BASE_DELAY * (2 ** (attempt - 1)),
                            SESSION_RETRY_MAX_DELAY,
                        )
                        _logger.warning(
                            f"Session expired. Attempt {attempt} of {max_attempts}. Refreshing session and retrying after {delay} seconds..."
                        )
                        self.refresh_client_and_session()
                        time.sleep(delay)
                        continue
                    else:
                        refresh_failure_message = f"Failed to execute {func.__name__} after {max_attempts} attempts due to session expiration."
                        raise RuntimeError(refresh_failure_message) from e
                else:
                    raise

    return wrapper


class DatabricksFunctionClient(BaseFunctionClient):
    """
    Databricks UC function calling client
    """

    def __init__(
        self,
        client: Optional["WorkspaceClient"] = None,
        *,
        warehouse_id: Optional[str] = None,
        profile: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """
        Databricks UC function calling client.

        Args:
            client: The databricks workspace client. If it's None, a default databricks workspace client
                is generated based on the configuration. Defaults to None.
            warehouse_id: The warehouse id to use for executing functions. This field is
                not needed if serverless is enabled in the databricks workspace. Defaults to None.
            profile: The configuration profile to use for databricks connect. Defaults to None.
        """
        self.client = client or get_default_databricks_workspace_client(profile=profile)
        self.warehouse_id = warehouse_id
        self._validate_warehouse_type()
        self.profile = profile
        # TODO: add CI to run this in Databricks notebook
        self.spark = _try_get_spark_session_in_dbr()
        self._is_default_client = client is None
        super().__init__()

    def _is_spark_session_active(self):
        if self.spark is None:
            return False
        if hasattr(self.spark, "is_stopped"):
            return not self.spark.is_stopped
        return self.spark.getActiveSession() is not None

    def set_default_spark_session(self):
        if not self._is_spark_session_active():
            _validate_databricks_connect_available()
            from databricks.connect.session import DatabricksSession as SparkSession

            if self.profile:
                builder = SparkSession.builder.profile(self.profile)
            else:
                builder = SparkSession.builder
            self.spark = builder.serverless(True).getOrCreate()

    def stop_spark_session(self):
        if self._is_spark_session_active():
            self.spark.stop()

    def _validate_warehouse_type(self):
        if (
            self.warehouse_id
            and not self.client.warehouses.get(self.warehouse_id).enable_serverless_compute
        ):
            raise ValueError(
                f"Warehouse {self.warehouse_id} does not support serverless compute. "
                "Please use a serverless warehouse following the instructions here: "
                "https://docs.databricks.com/en/admin/sql/serverless.html#enable-serverless-sql-warehouses."
            )

    def refresh_client_and_session(self):
        """
        Refreshes the databricks client and spark session if the session_id has been invalidated due to expiration of temporary credentials.
        If the client is running within an interactive Databricks notebook environment, the spark session is not terminated.
        """

        _logger.info("Refreshing Databricks client and Spark session due to session expiration.")
        self.client = get_default_databricks_workspace_client(profile=self.profile)
        if not _is_in_databricks_notebook_environment:
            self.stop_spark_session()
            self.set_default_spark_session()
        self.spark = _try_get_spark_session_in_dbr()

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
            self.set_default_spark_session()
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
        self, *, func: Callable[..., Any], catalog: str, schema: str, replace: bool = False
    ) -> "FunctionInfo":
        # TODO: migrate this guide to the documentation
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
            func (Callable): The Python function to convert into a UDF.
            catalog (str): The catalog name in which to create the function.
            schema (str): The schema name in which to create the function.
            replace (bool): Whether to replace the function if it already exists. Defaults to False.

        Returns:
            FunctionInfo: Metadata about the created function, including its name and signature.
        """

        if not callable(func):
            raise ValueError("The provided function is not callable.")

        sql_function_body = generate_sql_function_body(func, catalog, schema, replace)

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
        full_func_name = FullFunctionName.validate_full_function_name(function_name)
        if "*" in full_func_name.function:
            raise ValueError(
                "function name cannot include *, to get all functions in a catalog and schema, "
                "please use list_functions API instead."
            )
        return self.client.functions.get(function_name)

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
        if self.warehouse_id:
            return self._execute_uc_functions_with_warehouse(function_info, parameters)
        else:
            return self._execute_uc_functions_with_serverless(function_info, parameters)

    @retry_on_session_expiration
    def _execute_uc_functions_with_warehouse(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any]
    ) -> FunctionExecutionResult:
        from databricks.sdk.service.sql import StatementState

        _logger.info("Executing function using client warehouse_id.")

        parametrized_statement = get_execute_function_sql_stmt(function_info, parameters)
        response = self.client.statement_execution.execute_statement(
            statement=parametrized_statement.statement,
            warehouse_id=self.warehouse_id,
            parameters=parametrized_statement.parameters,
            wait_timeout=UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT.get(),
            row_limit=int(UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT.get()),
            byte_limit=int(UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT.get()),
        )
        # TODO: the first time the warehouse is invoked, it might take longer than
        # expected, so it's still pending even after 6 times of retry;
        # we should see if we can check the warehouse status before invocation, and
        # increase the wait time if needed
        if response.status and job_pending(response.status.state) and response.statement_id:
            statement_id = response.statement_id
            _logger.info("Retrying to get statement execution status...")
            wait_time = 0
            retry_cnt = 0
            client_execution_timeout = int(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.get())
            while wait_time < client_execution_timeout:
                wait = min(2**retry_cnt, client_execution_timeout - wait_time)
                time.sleep(wait)
                _logger.info(f"Retry times: {retry_cnt}")
                response = self.client.statement_execution.get_statement(statement_id)
                if response.status is None or not job_pending(response.status.state):
                    break
                wait_time += wait
                retry_cnt += 1
            if response.status and job_pending(response.status.state):
                return FunctionExecutionResult(
                    error=f"Statement execution is still {response.status.state.value.lower()} after {wait_time} "
                    "seconds. Please increase the wait_timeout argument for executing "
                    f"the function or increase {UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name} environment "
                    f"variable for increasing retrying time, default value is {UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.default_value} seconds."
                )
        if response.status is None:
            return FunctionExecutionResult(error=f"Statement execution failed: {response}")
        if response.status.state != StatementState.SUCCEEDED:
            error = response.status.error
            if error is None:
                return FunctionExecutionResult(
                    error=f"Statement execution failed but no error message was provided: {response}"
                )
            return FunctionExecutionResult(error=f"{error.error_code}: {error.message}")

        manifest = response.manifest
        if manifest is None:
            return FunctionExecutionResult(
                error="Statement execution succeeded but no manifest was returned."
            )
        truncated = manifest.truncated
        if response.result is None:
            return FunctionExecutionResult(
                error="Statement execution succeeded but no result was provided."
            )
        data_array = response.result.data_array
        if is_scalar(function_info):
            value = None
            if data_array and len(data_array) > 0 and len(data_array[0]) > 0:
                # value is always string type
                value = data_array[0][0]
            return FunctionExecutionResult(format="SCALAR", value=value, truncated=truncated)
        else:
            try:
                import pandas as pd
            except ImportError as e:
                raise ImportError(
                    "Could not import pandas python package. Please install it with `pip install pandas`."
                ) from e

            schema = manifest.schema
            if schema is None or schema.columns is None:
                return FunctionExecutionResult(
                    error="Statement execution succeeded but no schema was provided for table function."
                )
            columns = [c.name for c in schema.columns]
            if data_array is None:
                data_array = []
            pdf = pd.DataFrame(data_array, columns=columns)
            csv_buffer = StringIO()
            pdf.to_csv(csv_buffer, index=False)
            return FunctionExecutionResult(
                format="CSV", value=csv_buffer.getvalue(), truncated=truncated
            )

    @retry_on_session_expiration
    def _execute_uc_functions_with_serverless(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any]
    ) -> FunctionExecutionResult:
        _logger.info("Using databricks connect to execute functions with serverless compute.")
        self.set_default_spark_session()
        sql_command = get_execute_function_sql_command(function_info, parameters)
        try:
            result = self.spark.sql(sqlQuery=sql_command.sql_query, args=sql_command.args or None)
            if is_scalar(function_info):
                return FunctionExecutionResult(format="SCALAR", value=str(result.collect()[0][0]))
            else:
                row_limit = int(UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT.get())
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
            "warehouse_id": self.warehouse_id,
            "profile": self.profile,
        }

    @classmethod
    def from_dict(cls, config: Dict[str, Any]):
        accept_keys = ["warehouse_id", "profile"]
        return cls(**{k: v for k, v in config.items() if k in accept_keys})


def is_scalar(function: "FunctionInfo") -> bool:
    from databricks.sdk.service.catalog import ColumnTypeName

    return function.data_type != ColumnTypeName.TABLE_TYPE


def job_pending(state: "StatementState") -> bool:
    from databricks.sdk.service.sql import StatementState

    return state in (StatementState.PENDING, StatementState.RUNNING)


@dataclass
class ParameterizedStatement:
    statement: str
    parameters: List["StatementParameterListItem"]


def get_execute_function_sql_stmt(
    function: "FunctionInfo", parameters: Dict[str, Any]
) -> ParameterizedStatement:
    from databricks.sdk.service.catalog import ColumnTypeName
    from databricks.sdk.service.sql import StatementParameterListItem

    parts: List[str] = []
    output_params: List[StatementParameterListItem] = []
    if is_scalar(function):
        parts.append("SELECT IDENTIFIER(:function_name)(")
        output_params.append(
            StatementParameterListItem(name="function_name", value=function.full_name)
        )
    else:
        # TODO: IDENTIFIER doesn't work
        parts.append(f"SELECT * FROM {function.full_name}(")

    if parameters and function.input_params and function.input_params.parameters:
        args: List[str] = []
        use_named_args = False
        for param_info in function.input_params.parameters:
            if param_info.name not in parameters:
                # validate_input_params has validated param_info.parameter_default exists
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
                    # Use from_json to restore values of complex types.
                    json_value_str = json.dumps(param_value)
                    arg_clause += f"from_json(:{param_info.name}, :{param_info.name}_type)"
                    output_params.append(
                        StatementParameterListItem(name=param_info.name, value=json_value_str)
                    )
                    output_params.append(
                        StatementParameterListItem(
                            name=f"{param_info.name}_type", value=param_info.type_text
                        )
                    )
                elif param_info.type_name == ColumnTypeName.BINARY:
                    if isinstance(param_value, bytes):
                        param_value = base64.b64encode(param_value).decode("utf-8")
                    # Use ubbase64 to restore binary values.
                    arg_clause += f"unbase64(:{param_info.name})"
                    output_params.append(
                        StatementParameterListItem(name=param_info.name, value=param_value)
                    )
                elif is_time_type(param_info.type_name.value):
                    date_str = (
                        param_value if isinstance(param_value, str) else param_value.isoformat()
                    )
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name, value=date_str, type=param_info.type_text
                        )
                    )
                elif param_info.type_name == ColumnTypeName.INTERVAL:
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name,
                            value=convert_timedelta_to_interval_str(param_value)
                            if not isinstance(param_value, str)
                            else param_value,
                            type=param_info.type_text,
                        )
                    )
                else:
                    if param_info.type_name == ColumnTypeName.DECIMAL and isinstance(
                        param_value, Decimal
                    ):
                        param_value = float(param_value)
                    arg_clause += f":{param_info.name}"
                    output_params.append(
                        StatementParameterListItem(
                            name=param_info.name, value=param_value, type=param_info.type_text
                        )
                    )
                args.append(arg_clause)
        parts.append(",".join(args))
    parts.append(")")
    statement = "".join(parts)
    return ParameterizedStatement(statement=statement, parameters=output_params)


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
