import base64
import json
import logging
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from typing_extensions import override

from ucai.core.client import BaseFunctionClient, FunctionExecutionResult
from ucai.core.envs.databricks_env_vars import (
    UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT,
    UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT,
)
from ucai.core.paged_list import PagedList
from ucai.core.utils.type_utils import (
    column_type_to_python_type,
    convert_timedelta_to_interval_str,
    is_time_type,
)
from ucai.core.utils.validation_utils import validate_full_function_name, validate_param

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import (
        CreateFunction,
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

        return spark
    except Exception:
        return


def extract_function_name(sql_body: str) -> str:
    """
    Extract function name from the sql body.
    CREATE FUNCTION syntax reference: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax
    """
    pattern = re.compile(
        r"""
        CREATE\s+(?:OR\s+REPLACE\s+)?      # Match 'CREATE OR REPLACE' or just 'CREATE'
        (?:TEMPORARY\s+)?                  # Match optional 'TEMPORARY'
        FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?  # Match 'FUNCTION' and optional 'IF NOT EXISTS'
        ([\w.]+)                           # Capture the function name (including schema if present)
        \s*\(                              # Match opening parenthesis after function name
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    match = pattern.search(sql_body)
    if match:
        return match.group(1)
    raise ValueError(
        f"Could not extract function name from the sql body {sql_body}.\nPlease "
        "make sure the sql body follows the syntax of CREATE FUNCTION "
        "statement in Databricks: "
        "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax."
    )


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
        super().__init__()

    def set_default_spark_session(self):
        if self.spark is None or self.spark.is_stopped:
            _validate_databricks_connect_available()
            from databricks.connect.session import DatabricksSession as SparkSession

            if self.profile:
                builder = SparkSession.builder.profile(self.profile)
            else:
                builder = SparkSession.builder
            self.spark = builder.serverless(True).getOrCreate()

    def stop_spark_session(self):
        if self.spark is not None and not self.spark.is_stopped:
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

    @override
    def create_function(
        self,
        *,
        sql_function_body: Optional[str] = None,
        function_info: Optional["CreateFunction"] = None,
    ) -> "FunctionInfo":
        """
        Create a UC function with the given sql body or function info.

        Note: `databricks-connect` is required to use this function, make sure its version is 15.1.0 or above to use
            serverless compute.

        Args:
            sql_function_body: The sql body of the function. Defaults to None.
                It should follow the syntax of CREATE FUNCTION statement in Databricks.
                Ref: https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html#syntax

            function_info: The function info. Defaults to None.

        Returns:
            FunctionInfo: The created function info.
        """
        if sql_function_body and function_info:
            raise ValueError("Only one of sql_function_body and function_info should be provided.")
        if sql_function_body:
            self.set_default_spark_session()
            try:
                # TODO: add timeout
                self.spark.sql(sql_function_body)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to create function with sql body: {sql_function_body}"
                ) from e
            return self.get_function(extract_function_name(sql_function_body))
        if function_info:
            # TODO: support this after CreateFunction bug is fixed in databricks-sdk
            raise NotImplementedError("Creating function using function_info is not supported yet.")
            return self.client.functions.create(function_info)
        raise ValueError("Either function_info or sql_function_body should be provided.")

    @override
    def create_python_function(
        self, *, func: Callable[..., Any], catalog: str, schema: str, replace: bool = False
    ) -> "FunctionInfo":
        raise NotImplementedError("Creating python function is not supported yet.")

    @override
    def get_function(self, function_name: str, **kwargs: Any) -> "FunctionInfo":
        """
        Get a function by its name.

        Args:
            function_name: The name of the function to get.
            kwargs: additional key-value pairs to include when getting the function.

        Note:
            The function name shouldn't be *, to get all functions in a catalog and schema,
            please use list_functions API instead.

        Returns:
            FunctionInfo: The function info.
        """
        full_func_name = validate_full_function_name(function_name)
        if "*" in full_func_name.function_name:
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
    ) -> PagedList["FunctionInfo"]:
        """
        List functions in a catalog and schema.

        Args:
            catalog: The catalog name.
            schema: The schema name.
            max_results: The maximum number of functions to return. Defaults to None.
            page_token: The token for the next page. Defaults to None.

        Returns:
            PageList[List[FunctionInfo]]: The paginated list of function infos.
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
                Allowed keys for retreiiving functions are:
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
                - byte_limit: int (default to 4096)
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
        if self.warehouse_id:
            return self._execute_uc_functions_with_warehouse(function_info, parameters)
        else:
            return self._execute_uc_functions_with_serverless(function_info, parameters)

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

    def _execute_uc_functions_with_serverless(
        self, function_info: "FunctionInfo", parameters: Dict[str, Any]
    ) -> FunctionExecutionResult:
        _logger.info("Using databricks connect to execute functions with serverless compute.")
        self.set_default_spark_session()
        sql_command = get_execute_function_sql_command(function_info, parameters)
        try:
            result = self.spark.sql(sqlQuery=sql_command)
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
            raise RuntimeError(
                f"Failed to execute function with function name: {function_info.full_name}"
            ) from e

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


def get_execute_function_sql_command(function: "FunctionInfo", parameters: Dict[str, Any]) -> str:
    from databricks.sdk.service.catalog import ColumnTypeName

    sql_query = ""
    if is_scalar(function):
        sql_query += f"SELECT `{function.catalog_name}`.`{function.schema_name}`.`{function.name}`("
    else:
        sql_query += (
            f"SELECT * FROM `{function.catalog_name}`.`{function.schema_name}`.`{function.name}`("
        )

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
                    arg_clause += f"'{str(param_value)}'"
                args.append(arg_clause)
        sql_query += ",".join(args)
    sql_query += ")"
    return sql_query
