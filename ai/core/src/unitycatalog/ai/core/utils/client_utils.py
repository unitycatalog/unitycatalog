from typing import Optional

from unitycatalog.ai.core.base import (
    BaseFunctionClient,
    get_uc_function_client,
)


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
    if client is None:
        raise ValueError(
            "No client provided, either set the client when creating a "
            "toolkit or set the default client using "
            "unitycatalog.ai.core.client.set_uc_function_client(client)."
        )
    return client
