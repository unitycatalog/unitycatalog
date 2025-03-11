import logging
from typing import Optional

from unitycatalog.ai.core.base import (
    BaseFunctionClient,
    get_uc_function_client,
    set_uc_function_client,
)

_logger = logging.getLogger(__name__)


def _is_databricks_client_available():
    """
    Checks if the connection requirements to attach to a Databricks serverless cluster
    are available in the environment for automatic client selection purposes in
    toolkit instantiation.

    Returns:
        bool: True if the requirements are available, False otherwise.
    """
    try:
        from databricks.connect.session import DatabricksSession

        if hasattr(DatabricksSession.builder, "serverless"):
            return True
    except Exception:
        return False


def validate_or_set_default_client(client: Optional[BaseFunctionClient] = None):
    """
    Validate or set the default client.
    If a client is provided, it returns the client. If not, it attempts to retrieve
    the default client using `get_uc_function_client()`. Raises a `ValueError` if no
    client is available.
    If a client can be created automatically to connect to Databricks, a default client
    is set to connect to Databricks.

    Args:
        client (Optional[BaseFunctionClient]): The client to validate or set.
            Defaults to None.

    Returns:
        BaseFunctionClient: The validated client.

    Raises:
        ValueError: If no client is provided and no default client is available.
    """

    client = client or get_uc_function_client()
    if client is None and _is_databricks_client_available():
        try:
            from unitycatalog.ai.core.databricks import DatabricksFunctionClient

            client = DatabricksFunctionClient()

        except Exception as e:
            raise ValueError(
                "Attempted to set DatabricksFunctionClient as the default client, but encountered an error. "
                "Provide a client directly to your toolkit invocation to ensure connection to Unity Catalog."
            ) from e

        set_uc_function_client(client)
        _logger.info("Client has been set to DatabricksFunctionClient with default configuration.")

    if client is None:
        raise ValueError(
            "No client provided, either set the client when creating a "
            "toolkit or set the default client using "
            "unitycatalog.ai.core.client.set_uc_function_client(client)."
        )
    return client
