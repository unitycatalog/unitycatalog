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
