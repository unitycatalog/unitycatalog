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
