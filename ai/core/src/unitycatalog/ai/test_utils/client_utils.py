import os
import time
from contextlib import contextmanager
from functools import wraps
from unittest import mock

import pytest

from unitycatalog.ai.core.base import set_uc_function_client
from unitycatalog.ai.core.databricks import DatabricksFunctionClient

TEST_IN_DATABRICKS = os.environ.get("TEST_IN_DATABRICKS", "false").lower() == "true"
PROFILE = os.environ.get("DATABRICKS_CONFIG_PROFILE")


def requires_databricks(test_func):
    return pytest.mark.skipif(
        not TEST_IN_DATABRICKS,
        reason="This function test relies on connecting to a databricks workspace",
    )(test_func)


@pytest.fixture
def client() -> DatabricksFunctionClient:
    if TEST_IN_DATABRICKS:
        return DatabricksFunctionClient(profile=PROFILE)
    else:
        with (
            mock.patch(
                "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
                return_value=mock.Mock(),
            ),
            mock.patch(
                "unitycatalog.ai.core.databricks.DatabricksFunctionClient.set_spark_session",
                lambda self: None,
            ),
        ):
            return DatabricksFunctionClient()


@pytest.fixture
def serverless_client() -> DatabricksFunctionClient:
    return DatabricksFunctionClient(profile=PROFILE)


@pytest.fixture
def serverless_client_with_config() -> DatabricksFunctionClient:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient(
        host=os.environ.get("DATABRICKS_HOST"),
        client_id=os.environ.get("DATABRICKS_CLIENT_ID"),
        client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET"),
    )
    return DatabricksFunctionClient(client=w)


def get_client() -> DatabricksFunctionClient:
    if TEST_IN_DATABRICKS:
        return DatabricksFunctionClient(profile=PROFILE)
    else:
        with (
            mock.patch(
                "unitycatalog.ai.core.databricks.get_default_databricks_workspace_client",
                return_value=mock.Mock(),
            ),
            mock.patch(
                "unitycatalog.ai.core.databricks.DatabricksFunctionClient.set_spark_session",
                lambda self: None,
            ),
        ):
            return DatabricksFunctionClient()


@contextmanager
def set_default_client(client: DatabricksFunctionClient):
    try:
        set_uc_function_client(client)
        yield
    finally:
        set_uc_function_client(None)


def retry_flaky_test(tries=3):
    """Retries a flaky test a specified number of times."""

    def flaky_test_func(test_func):
        @wraps(test_func)
        def decorated_func(*args, **kwargs):
            for i in range(tries):
                try:
                    return test_func(*args, **kwargs)
                except Exception as e:
                    if i == tries - 1:
                        raise e
                    time.sleep(2)

        return decorated_func

    return flaky_test_func
