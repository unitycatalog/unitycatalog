import os


class _EnvironmentVariable:
    def __init__(self, name: str, default_value: str, description: str):
        self.name = name
        self.default_value = str(default_value)
        self.description = description

    def get(self) -> str:
        return os.getenv(self.name, self.default_value)

    def set(self, value: str) -> None:
        os.environ[self.name] = str(value)

    def remove(self) -> None:
        os.environ.pop(self.name, None)

    def __repr__(self) -> str:
        return f"Environment variable for {self.name}. Default value: {self.default_value}. " + (
            f"Usage: {self.description}" if self.description else ""
        )


UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT",
    "30s",
    "The time in seconds the call will wait for the function to execute using Databricks client with warehouse_id; "
    "the value should be set as `Ns`, where `N` can be set to 0 or to a value between 5 and 50.",
)
UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_ROW_LIMIT",
    "100",
    "Maximum number of rows of the function execution result using Databricks client with warehouse_id; "
    "it also sets the `truncated` field in the response to indicate whether the result was trimmed due to the limit or not.",
)
UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_BYTE_LIMIT",
    "1048576",  # 1MB = 1024 * 1024
    "Maximum byte size of the function execution result using Databricks client with warehouse_id; "
    "If the result was truncated due to the byte limit, then `truncated` in the response is set to `true`.",
)
UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT = _EnvironmentVariable(
    "UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT",
    "120",
    "Client side retry timeout for the function execution using Databricks client with warehouse_id; "
    "if the function execution does not complete within UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT, client side will "
    "retry to fetch the result until this timeout is reached.",
)
UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT = _EnvironmentVariable(
    "UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT",
    "100",
    "Maximum number of rows of the function execution result using Databricks client with serverless.",
)
UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS = _EnvironmentVariable(
    "UCAI_DATABRICKS_SESSION_RETRY_MAX_ATTEMPTS",
    "5",
    "Maximum number of attempts to retry refreshing the session client in case of token expiry.",
)
