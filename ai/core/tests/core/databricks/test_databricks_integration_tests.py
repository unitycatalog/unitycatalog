import json
import math
import os
from typing import Callable, Dict, List

import pytest
from databricks.sdk.errors import ResourceDoesNotExist

from tests.core.databricks.function_definitions import (
    FunctionInputOutput,
    PythonFunctionInputOutput,
    function_with_array_input,
    function_with_binary_input,
    function_with_date_input,
    function_with_decimal_input,
    function_with_interval_input,
    function_with_map_input,
    function_with_scalar_retriever_output,
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
)
from unitycatalog.ai.test_utils.client_utils import (
    TEST_IN_DATABRICKS,
    client,  # noqa: F401
    get_client,
    requires_databricks,
    retry_flaky_test,
    serverless_client,  # noqa: F401
)
from unitycatalog.ai.test_utils.function_utils import (
    CATALOG,
    create_function_and_cleanup,
    create_python_function_and_cleanup,
    generate_func_name_and_cleanup,
    random_func_name,
)

SCHEMA = os.environ.get("SCHEMA", "ucai_core_test")


@retry_flaky_test()
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


@retry_flaky_test()
@requires_databricks
def test_create_and_execute_retriever_function(serverless_client: DatabricksFunctionClient):
    import mlflow

    if TEST_IN_DATABRICKS:
        import mlflow.tracking._model_registry.utils

        mlflow.tracking._model_registry.utils._get_registry_uri_from_spark_session = (
            lambda: "databricks-uc"
        )

    with generate_func_name_and_cleanup(serverless_client, schema=SCHEMA) as func_name:
        function_sample = function_with_scalar_retriever_output(func_name)
        serverless_client.create_function(sql_function_body=function_sample.sql_body)
        for input_example in function_sample.inputs:
            result = serverless_client.execute_function(
                func_name, input_example, enable_retriever_tracing=True
            )
            assert result.value == function_sample.output, result.error

            trace = mlflow.get_last_active_trace()
            assert trace is not None
            assert trace.info.execution_time_ms is not None
            assert trace.data.request == json.dumps(input_example)
            assert trace.data.response == function_sample.output.replace("'", '"')


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
@requires_databricks
def test_create_and_execute_python_function(client: DatabricksFunctionClient):
    def simple_func(x: int) -> str:
        """Test function that returns the string version of x."""
        return str(x)

    with create_python_function_and_cleanup(client, func=simple_func, schema=SCHEMA) as func_obj:
        result = client.execute_function(func_obj.full_function_name, {"x": 10})
        assert result.value == "10"


@retry_flaky_test()
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


@retry_flaky_test()
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


@retry_flaky_test()
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
        assert result.value == '[0, 1, 2]'


@retry_flaky_test()
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
        assert result.value == "{'key_0': 0,'key_1': 1,'key_2': 2}"


@retry_flaky_test()
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


@retry_flaky_test()
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


integration_test_cases = [
    ("\nprint('Hello World!')", "Hello World!\n"),
    ("def greet(name='Bob'):\n    return f'Hello {name}!'\nprint(greet())", "Hello Bob!\n"),
    ("for i in range(5):\n\tif i % 2 == 0:\n\t\tprint(i)", "0\n2\n4\n"),
    (
        """def calculate_sum(numbers):
\t\ttotal = 0
\t\tfor num in numbers:
\t\t\ttotal += num
\t\treturn total
print(calculate_sum([1, 2, 3, 4, 5]))""",
        "15\n",
    ),
    # Simple print statement
    ("print('Hello, world!')", "Hello, world!\n"),
    # Code with double quotes
    ('print("He said, \\"Hi!\\"")', 'He said, "Hi!"\n'),
    # Code with backslashes
    (r"print('C:\\path\\into\\dir')", "C:\\path\\into\\dir\n"),
    # Multi-line code with newlines
    ("for i in range(3):\n    print(i)", "0\n1\n2\n"),
    # Code with tabs and indents
    ("def greet(name):\n    print(f'Hello, {name}!')\ngreet('Alice')", "Hello, Alice!\n"),
    # Code with special characters
    ("print('Special chars: !@#$%^&*()')", "Special chars: !@#$%^&*()\n"),
    # Unicode characters
    ("print('Unicode test: ü, é, 漢字')", "Unicode test: ü, é, 漢字\n"),
    # Code with comments
    ("# This is a comment\nprint('Comment test')", "Comment test\n"),
    # Code raising an exception
    (
        "try:\n    raise ValueError('Test error')\nexcept Exception as e:\n    print(f'Caught an error: {e}')",
        "Caught an error: Test error\n",
    ),
    # Code with triple quotes
    ('print("""Triple quote test""")', "Triple quote test\n"),
    # Code with raw strings
    ("print('Raw string: \\\\n new line')", "Raw string: \\n new line\n"),
    # Empty code string
    ("", ""),
    # Code with carriage return
    ("print('Line1\\\\rLine2')", "Line1\\rLine2\n"),
    # Code with encoding declarations (Note: encoding declarations should be in the first or second line)
    ("# -*- coding: utf-8 -*-\nprint('Encoding test')", "Encoding test\n"),
    # Code importing a standard library
    ("import math\nprint(math.pi)", f"{math.pi}\n"),
    # Code with nested functions
    (
        "def outer():\n    def inner():\n        return 'Nested'\n    return inner()\nprint(outer())",
        "Nested\n",
    ),
    # Code with list comprehensions
    ("squares = [x**2 for x in range(5)]\nprint(squares)", "[0, 1, 4, 9, 16]\n"),
    # Code with multi-line strings
    ("multi_line = '''Line1\nLine2\nLine3'''\nprint(multi_line)", "Line1\nLine2\nLine3\n"),
]


@requires_databricks
@pytest.mark.parametrize("code, expected_output", integration_test_cases)
def test_execute_python_code_integration(code: str, expected_output: str):
    client = get_client()

    def python_exec(code: str) -> str:
        """
        Execute the provided Python code and return the output.
        """
        import sys
        from io import StringIO

        sys_stdout = sys.stdout
        redirected_output = StringIO()
        sys.stdout = redirected_output

        exec(code)
        sys.stdout = sys_stdout
        return redirected_output.getvalue()

    function_full_name = f"{CATALOG}.{SCHEMA}.python_exec"

    with create_python_function_and_cleanup(client, func=python_exec, schema=SCHEMA):
        result = client.execute_function(
            function_name=function_full_name, parameters={"code": code}
        )

        assert result.error is None, f"Function execution failed with error: {result.error}"

        assert result.value == expected_output


@requires_databricks
@pytest.mark.parametrize(
    "text",
    [
        "MLflow is an open-source platform for managing the end-to-end machine learning lifecycle. It was developed by Databricks and is now a part of the Linux Foundation's AI Foundation.",
        "print('Hello, \"world!\"')",
        "'return '2' + \"" '3"' "' is a valid input to this function",
    ],
)
def test_string_param_passing_work(text: str):
    client = get_client()
    function_name = random_func_name(schema=SCHEMA)
    summarize_in_20_words = f"""CREATE OR REPLACE FUNCTION {function_name}(text STRING)
RETURNS STRING
RETURN SELECT ai_summarize(text, 20)
"""
    with create_function_and_cleanup(client=client, schema=SCHEMA, sql_body=summarize_in_20_words):
        result = client.execute_function(function_name, {"text": text})
        assert result.error is None, f"Function execution failed with error: {result.error}"
        # number of words should be no more than 20
        assert len(result.value.split(" ")) <= 20
