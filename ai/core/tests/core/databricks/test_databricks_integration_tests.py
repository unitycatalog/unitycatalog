import os
import time
from typing import Callable, Dict, List

import pytest
from databricks.sdk.errors import ResourceDoesNotExist

from tests.core.function_definitions import (
    FunctionInputOutput,
    PythonFunctionInputOutput,
    function_with_array_input,
    function_with_binary_input,
    function_with_date_input,
    function_with_decimal_input,
    function_with_interval_input,
    function_with_map_input,
    function_with_string_input,
    function_with_struct_input,
    function_with_table_output,
    function_with_timestamp_input,
    python_function_with_array_input,
    python_function_with_binary_input,
    python_function_with_date_input,
    python_function_with_decimal_input,
    python_function_with_dict_input,
    python_function_with_interval_input,
    python_function_with_map_input,
    python_function_with_string_input,
    python_function_with_timestamp_input,
    simple_sql_function_boy,
)
from unitycatalog.ai.core.databricks import (
    DatabricksFunctionClient,
)
from unitycatalog.ai.core.envs.databricks_env_vars import (
    UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT,
    UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT,
    UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT,
)
from unitycatalog.ai.test_utils.client_utils import (
    client,  # noqa: F401
    requires_databricks,
    serverless_client,  # noqa: F401
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_python_function_and_cleanup,
    generate_func_name_and_cleanup,
    random_func_name,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        function_with_array_input,
        function_with_struct_input,
        function_with_string_input,
        function_with_binary_input,
        function_with_interval_input,
        function_with_timestamp_input,
        function_with_date_input,
        function_with_map_input,
        function_with_decimal_input,
        function_with_table_output,
    ],
)
def test_create_and_execute_function(
    client: DatabricksFunctionClient, create_function: Callable[[str], FunctionInputOutput]
):
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        function_sample = create_function(func_name)
        client.create_function(sql_function_body=function_sample.sql_body)
        for input_example in function_sample.inputs:
            result = client.execute_function(func_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        function_with_array_input,
        function_with_struct_input,
        function_with_string_input,
        function_with_binary_input,
        function_with_interval_input,
        function_with_timestamp_input,
        function_with_date_input,
        function_with_map_input,
        function_with_decimal_input,
        function_with_table_output,
    ],
)
def test_create_and_execute_function_using_serverless(
    serverless_client: DatabricksFunctionClient,
    create_function: Callable[[str], FunctionInputOutput],
):
    with generate_func_name_and_cleanup(serverless_client, schema=SCHEMA) as func_name:
        function_sample = create_function(func_name)
        serverless_client.create_function(sql_function_body=function_sample.sql_body)
        for input_example in function_sample.inputs:
            result = serverless_client.execute_function(func_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
@pytest.mark.parametrize(
    "create_function",
    [
        python_function_with_dict_input,
        python_function_with_array_input,
        python_function_with_string_input,
        python_function_with_binary_input,
        python_function_with_interval_input,
        python_function_with_timestamp_input,
        python_function_with_date_input,
        python_function_with_map_input,
        python_function_with_decimal_input,
    ],
)
def test_create_and_execute_python_function(
    client: DatabricksFunctionClient, create_function: Callable[[], PythonFunctionInputOutput]
):
    function_sample = create_function()
    with create_python_function_and_cleanup(
        client, func=function_sample.func, schema=SCHEMA
    ) as func_obj:
        for input_example in function_sample.inputs:
            result = client.execute_function(func_obj.full_function_name, input_example)
            assert result.value == function_sample.output


@requires_databricks
def test_execute_function_using_serverless_row_limit(
    serverless_client: DatabricksFunctionClient,
    monkeypatch,
):
    monkeypatch.setenv(UCAI_DATABRICKS_SERVERLESS_EXECUTION_RESULT_ROW_LIMIT.name, "1")
    with generate_func_name_and_cleanup(serverless_client, schema=SCHEMA) as func_name:
        function_sample = function_with_table_output(func_name)
        serverless_client.create_function(sql_function_body=function_sample.sql_body)
        result = serverless_client.execute_function(func_name, function_sample.inputs[0])
        assert result.value == "day_of_week,day\n1,2024-01-01\n"
        assert result.truncated is True


@requires_databricks
def test_execute_function_with_timeout(client: DatabricksFunctionClient, monkeypatch):
    monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "5")
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}()
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import time

    time.sleep(100)
    return "10"
$$
"""
        client.create_function(sql_function_body=sql_body)
        result = client.execute_function(func_name)
        assert result.error.startswith("Statement execution is still running after 5 seconds")

        monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "100")
        result = client.execute_function(func_name)
        assert result.value == "10"


@requires_databricks
def test_get_function(client: DatabricksFunctionClient):
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}(s STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    return s
    $$
"""
        create_func_info = client.create_function(sql_function_body=sql_body)
        function_info = client.get_function(func_name)
        assert create_func_info == function_info


@requires_databricks
def test_list_functions(client: DatabricksFunctionClient):
    function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA)

    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        create_func_info = client.create_function(
            sql_function_body=simple_sql_function_boy(func_name)
        )
        function_info = client.get_function(func_name)
        assert create_func_info == function_info

        function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA)
        assert len([f for f in function_infos if f.full_name == func_name]) == 1

        with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name_2:
            client.create_function(sql_function_body=simple_sql_function_boy(func_name_2))
            function_infos = client.list_functions(catalog=CATALOG, schema=SCHEMA, max_results=1)
            assert len(function_infos) == 1
            function_info = function_infos[0]
            function_infos = client.list_functions(
                catalog=CATALOG, schema=SCHEMA, max_results=1, page_token=function_infos.token
            )
            assert len(function_infos) == 1
            assert function_infos[0] != function_info


@requires_databricks
def test_delete_function(serverless_client: DatabricksFunctionClient):
    function_name = random_func_name(schema=SCHEMA)
    with pytest.raises(ResourceDoesNotExist, match=rf"'{function_name}' does not exist"):
        serverless_client.delete_function(function_name)

    serverless_client.create_function(sql_function_body=simple_sql_function_boy(function_name))
    serverless_client.get_function(function_name)
    serverless_client.delete_function(function_name)
    with pytest.raises(ResourceDoesNotExist, match=rf"'{function_name}' does not exist"):
        serverless_client.get_function(function_name)


@requires_databricks
def test_extra_params_when_executing_function_e2e(client: DatabricksFunctionClient, monkeypatch):
    monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_RETRY_TIMEOUT.name, "5")
    with generate_func_name_and_cleanup(client, schema=SCHEMA) as func_name:
        sql_body = f"""CREATE FUNCTION {func_name}()
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import time

    time.sleep(100)
    return "10"
$$
"""
        client.create_function(sql_function_body=sql_body)
        time1 = time.time()
        # default wait_timeout is 30s
        client.execute_function(func_name)
        time_total1 = time.time() - time1

        monkeypatch.setenv(UCAI_DATABRICKS_WAREHOUSE_EXECUTE_FUNCTION_WAIT_TIMEOUT.name, "10s")
        time2 = time.time()
        client.execute_function(func_name)
        time_total2 = time.time() - time2
        # 30s - 10s = 20s, the time difference should be around 20s
        assert abs(abs(time_total2 - time_total1) - 20) < 5


@requires_databricks
def test_create_and_execute_python_function(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"x": 10})
        assert result.value == "10"


@requires_databricks
def test_create_python_function_with_complex_body(client: DatabricksFunctionClient):
    def complex_func(a: int, b: int) -> int:
        """A complex function that uses a try-except block and returns the sum."""
        try:
            return a + b
        except Exception as e:
            raise ValueError(f"Failed to add numbers") from e

    with create_python_function_and_cleanup(client, func=complex_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 1, "b": 2})
        assert result.value == "3"


@requires_databricks
def test_create_python_function_with_docstring_comments(client: DatabricksFunctionClient):
    def documented_func(a: int, b: int) -> int:
        """
        Adds two integers.

        Args:
            a: The first integer.
            b: The second integer.

        Returns:
            int: The sum of a and b.
        """
        return a + b

    with create_python_function_and_cleanup(
        client, func=documented_func, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 5, "b": 3})
        assert result.value == "8"


@requires_databricks
def test_function_with_list_of_int_return(client: DatabricksFunctionClient):
    def func_returning_list(a: int) -> List[int]:
        """
        A function that returns a list of integers.

        Args:
            a: An integer to generate the list.

        Returns:
            List[int]: A list of integers from 0 to a.
        """
        return list(range(a))

    with create_python_function_and_cleanup(
        client, func=func_returning_list, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 3})
        # result wrapped as string is due to sql statement execution response parsing
        assert result.value == '["0","1","2"]'


@requires_databricks
def test_function_with_dict_of_string_to_int_return(client: DatabricksFunctionClient):
    def func_returning_map(a: int) -> Dict[str, int]:
        """
        A function that returns a map from string to integer.

        Args:
            a: The integer to use in generating the map.

        Returns:
            Dict[str, int]: A map of string keys to integer values.
        """
        return {f"key_{i}": i for i in range(a)}

    with create_python_function_and_cleanup(
        client, func=func_returning_map, schema=SCHEMA
    ) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"a": 3})
        # result wrapped as string is due to sql statement execution response parsing
        assert result.value == '{"key_0":"0","key_1":"1","key_2":"2"}'


@requires_databricks
def test_replace_existing_function(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    # Create the function for the first time
    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"x": 42})
        assert result.value == "42"

        # Modify the function definition
        def simple_func(x: int) -> str:
            """Modified function that returns 'Modified: ' plus the string version of x."""
            return f"Modified: {x}"

        # Replace the existing function
        client.create_python_function(
            func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=True
        )

        # Execute the function again to verify it has been replaced
        result = client.execute_function(func_obj.full_function_name, {"x": 42})
        assert result.value == "Modified: 42"


@requires_databricks
def test_create_function_without_replace(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    # Create the function for the first time
    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA):
        # Attempt to create the same function again without replace
        with pytest.raises(
            Exception,
            match=f"`{CATALOG}`.`{SCHEMA}`.`simple_func` because it already exists",
        ):
            client.create_python_function(
                func=simple_func, catalog=CATALOG, schema=SCHEMA, replace=False
            )
