from unitycatalog.ai.core.envs.base import _EnvironmentVariable

UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT",
    int,
    100,
    "Maximum number of rows of the function execution result using Databricks client with serverless.",
)
UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS = _EnvironmentVariable(
    "UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS",
    int,
    5,
    "Maximum number of attempts to retry refreshing the session client in case of token expiry.",
)
UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT",
    str,
    "30s",
    "Wait timeout for SQL warehouse statement execution when using warehouse_id.",
)
UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT",
    int,
    100,
    "Maximum number of rows returned by SQL warehouse statement execution when using warehouse_id.",
)
UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT",
    int,
    1048576,
    "Maximum number of bytes returned by SQL warehouse statement execution when using warehouse_id.",
)
UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT",
    int,
    120,
    "Total retry timeout in seconds when polling for warehouse statement completion when using warehouse_id.",
)
