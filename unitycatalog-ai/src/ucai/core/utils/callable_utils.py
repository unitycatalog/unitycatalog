import ast
import inspect
from dataclasses import dataclass
from textwrap import dedent, indent
from typing import Any, Callable, Optional, Union, get_args, get_origin, get_type_hints

from ucai.core.utils.type_utils import python_type_to_sql_type

FORBIDDEN_PARAMS = ["self", "cls"]


@dataclass
class DocstringInfo:
    """Dataclass to store parsed docstring information."""

    description: str
    params: Optional[dict[str, str]]
    returns: Optional[str]


class FunctionBodyExtractor(ast.NodeVisitor):
    """
    AST NodeVisitor class to extract the body of a function.
    """

    def __init__(self, func_name: str, source_code: str):
        self.func_name = func_name
        self.source_code = source_code
        self.function_body = ""
        self.indent_unit = 4
        self.found = False

    def visit_FunctionDef(self, node: ast.FunctionDef):
        if not self.found and node.name == self.func_name:
            self.found = True
            self.extract_body(node)

    def extract_body(self, node: ast.FunctionDef):
        body = node.body
        # Skip the docstring
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]

        if not body:
            return

        start_lineno = body[0].lineno
        end_lineno = body[-1].end_lineno

        source_lines = self.source_code.splitlines(keepends=True)
        function_body_lines = source_lines[start_lineno - 1 : end_lineno]

        self.function_body = dedent("".join(function_body_lines)).rstrip("\n")

        indents = [stmt.col_offset for stmt in body if stmt.col_offset is not None]
        if indents:
            self.indent_unit = min(indents)


class State:
    DESCRIPTION = "DESCRIPTION"
    ARGS = "ARGS"
    RETURNS = "RETURNS"
    END = "END"


def parse_docstring(docstring: str) -> DocstringInfo:
    """
    Parses the docstring to extract the function description, parameter comments,
    and return value description.
    Handles both reStructuredText and Google-style docstrings.

    Args:
        docstring: The docstring to parse.

    Returns:
        DocstringInfo: A dataclass containing the parsed information.

    Raises:
        ValueError: If the docstring is empty or missing a function description.
    """

    if not docstring or not docstring.strip():
        raise ValueError(
            "Docstring is empty. Please provide a docstring with a function description."
        )

    description_lines = []
    parsed_params = {}
    returns = None
    current_param = None
    param_description_lines = []

    state = State.DESCRIPTION
    lines = docstring.strip().splitlines()
    lines.append("")  # Add an empty line to ensure the last param is processed
    iterator = iter(lines)

    for line in iterator:
        stripped_line = line.strip()

        if stripped_line in ("Args:", "Arguments:"):
            state = State.ARGS
            continue
        elif stripped_line == "Returns:":
            if current_param and param_description_lines:
                parsed_params[current_param] = " ".join(param_description_lines).strip()
                current_param = None
                param_description_lines = []
            state = State.RETURNS
            continue
        elif stripped_line == "" and state == State.ARGS and current_param:
            parsed_params[current_param] = " ".join(param_description_lines).strip()
            current_param = None
            param_description_lines = []
            continue

        if state == State.DESCRIPTION:
            if stripped_line:
                description_lines.append(stripped_line)
        elif state == State.ARGS:
            if stripped_line:
                if ":" in stripped_line:
                    if current_param and param_description_lines:
                        parsed_params[current_param] = " ".join(param_description_lines).strip()
                    param_parts = stripped_line.split(":", 1)
                    # Remove type hints in parentheses if any
                    current_param = param_parts[0].strip().split()[0]
                    param_description_lines = [param_parts[1].strip()]
                else:
                    param_description_lines.append(stripped_line)
        elif state == State.RETURNS:
            if stripped_line:
                if returns is None:
                    returns = stripped_line
                else:
                    returns += " " + stripped_line

    if current_param and param_description_lines:
        parsed_params[current_param] = " ".join(param_description_lines).strip()

    description = " ".join(description_lines).strip()

    if not description:
        raise ValueError(
            "Function description is missing in the docstring. Please provide a function description."
        )

    return DocstringInfo(description=description, params=parsed_params, returns=returns)


def extract_function_body(func: Callable[..., Any]) -> tuple[str, int]:
    """
    Extracts the body of a function as a string without the signature or docstring,
    dedents the code, and returns the indentation unit used in the function (e.g., 2 or 4 spaces).
    """
    source_lines, _ = inspect.getsourcelines(func)
    dedented_source = dedent("".join(source_lines))
    func_name = func.__name__

    extractor = FunctionBodyExtractor(func_name, dedented_source)
    parsed_source = ast.parse(dedented_source)
    extractor.visit(parsed_source)

    return extractor.function_body, extractor.indent_unit


def validate_type_hint(hint: Any) -> str:
    """Validates and returns the SQL type for a given type hint."""
    # Handle typing.Optional, which is Union[type, None]
    if hasattr(hint, "__origin__") and hint.__origin__ is Union:
        non_none_types = [t for t in hint.__args__ if t is not type(None)]
        if len(non_none_types) == 1:
            return python_type_to_sql_type(non_none_types[0])
        else:
            raise ValueError(f"Unsupported type union: {hint}")
    if hint is Any:
        raise ValueError(
            "Unsupported Python type: typing.Any is not allowed. Please specify a concrete type."
        )
    return python_type_to_sql_type(hint)


def generate_type_hint_error_message(param_name: str, param_hint: Any, exception: Exception) -> str:
    """
    Generate an informative error message for unsupported parameter types, especially for Lists, Tuples, and Dicts.
    Args:
        param_name: The name of the parameter with the type hint issue.
        param_hint: The unsupported type hint.
        exception: The original exception raised.
    Returns:
        str: A detailed error message guiding the user on how to resolve the issue.
    """
    if hasattr(param_hint, "__origin__"):
        origin = param_hint.__origin__
        if origin == list:
            return (
                f"Error in parameter '{param_name}': List type requires a specific element type. "
                f"Please define the internal type for the list, e.g., List[int]. Original error: {exception}"
            )
        elif origin == tuple:
            return (
                f"Error in parameter '{param_name}': Tuple type requires specific element types. "
                f"Please define the types for the tuple elements, e.g., Tuple[int, str]. Original error: {exception}"
            )
        elif origin == dict:
            return (
                f"Error in parameter '{param_name}': Dict type requires both key and value types. "
                f"Please define the internal types for the dict, e.g., Dict[str, int]. Original error: {exception}"
            )
    return (
        f"Error in parameter '{param_name}': type {param_hint} is not supported. "
        f"Original error: {exception}"
    )


def format_default_value(default: Any) -> str:
    """Formats the default value for SQL."""
    if default is None:
        return "NULL"
    elif isinstance(default, str):
        return f"'{default}'"
    else:
        return str(default)


def is_collection_type(type_hint: Any) -> bool:
    """Checks if the type hint represents a collection type (list, tuple, dict)."""
    origin = get_origin(type_hint)
    return origin in (list, tuple, dict) or type_hint in (list, tuple, dict)


def unwrap_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """Unwraps staticmethod or classmethod to get the actual function."""
    if isinstance(func, (staticmethod, classmethod)):
        func = func.__func__
    return func


def is_valid_default_value(default: Any, type_hint: Any) -> bool:
    """
    Checks if the default value matches the type hint for scalar types.
    Handles Optional types by checking against each type in the Union.

    Note: this function is required for compatibility with Python 3.9 and lower
    due to the inability to use isinstance with subscripted generic types.
    This function can be removed and the type hint check can be done directly
    with isinstance(default, type_hint) when the minimum supported version of Python is 3.10+.
    """
    origin = get_origin(type_hint)
    args = get_args(type_hint)

    if default is None:
        if origin is Union and type(None) in args:
            return True
        elif type_hint is type(None):
            return True
        else:
            return False
    else:
        if origin is Union:
            return any(
                is_valid_default_value(default, arg) for arg in args if arg is not type(None)
            )
        elif isinstance(type_hint, type):
            return isinstance(default, type_hint)
        else:
            return False


def process_parameter(
    param_name: str,
    param: inspect.Parameter,
    type_hints: dict[str, Any],
    docstring_info: DocstringInfo,
) -> str:
    """Processes a single parameter and returns its SQL definition."""
    if param_name in FORBIDDEN_PARAMS:
        raise ValueError(f"Parameter '{param_name}' is not allowed in the function signature.")

    if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
        kind = (
            "var-positional (*args)"
            if param.kind == inspect.Parameter.VAR_POSITIONAL
            else "var-keyword (**kwargs)"
        )
        raise ValueError(
            f"Parameter '{param_name}' is a {kind} parameter, which is not supported in SQL functions."
        )

    if param_name not in type_hints:
        raise ValueError(f"Missing type hint for parameter: {param_name}.")

    param_hint = type_hints[param_name]

    try:
        sql_type = validate_type_hint(param_hint)
    except ValueError as e:
        error_message = generate_type_hint_error_message(param_name, param_hint, e)
        raise ValueError(error_message) from e

    param_comment = docstring_info.params.get(param_name, f"Parameter {param_name}").replace(
        "'", '"'
    )

    if param.default is not inspect.Parameter.empty:
        if is_collection_type(param_hint):
            raise ValueError(
                f"Parameter '{param_name}' of type '{param_hint}' cannot have a default value."
            )
        if not is_valid_default_value(param.default, param_hint):
            raise ValueError(
                f"Default value for parameter '{param_name}' does not match the type hint '{param_hint}'."
            )
        default_value = format_default_value(param.default)
        return f"{param_name} {sql_type} DEFAULT {default_value} COMMENT '{param_comment}'"
    else:
        return f"{param_name} {sql_type} COMMENT '{param_comment}'"


def assemble_sql_body(
    catalog: str,
    schema: str,
    func_name: str,
    sql_params: list[str],
    sql_return_type: str,
    func_comment: str,
    indented_body: str,
    replace: bool,
) -> str:
    replace_command = "CREATE OR REPLACE" if replace else "CREATE"
    """Assembles the final SQL function body."""
    sql_params_str = ", ".join(sql_params)
    sql_body = f"""
{replace_command} FUNCTION {catalog}.{schema}.{func_name}({sql_params_str})
RETURNS {sql_return_type}
LANGUAGE PYTHON
COMMENT '{func_comment}'
AS $$
{indented_body}
$$;
    """
    return sql_body


def generate_sql_function_body(
    func: Callable[..., Any], catalog: str, schema: str, replace: bool = False
) -> str:
    """
    Generate SQL body for creating the function in Unity Catalog.

    Args:
        func: The Python callable function to convert into a UDF.
        catalog: The catalog name.
        schema: The schema name.

    Returns:
        str: SQL statement for creating the UDF.
    """
    func = unwrap_function(func)
    func_name = func.__name__
    signature = inspect.signature(func)
    type_hints = get_type_hints(func)

    sql_return_type = validate_return_type(func_name, type_hints)

    docstring = inspect.getdoc(func)
    if not docstring:
        raise ValueError(f"Function '{func_name}' must have a docstring with a description.")
    docstring_info = parse_docstring(docstring)

    sql_params = []
    for param_name, param in signature.parameters.items():
        sql_param = process_parameter(param_name, param, type_hints, docstring_info)
        sql_params.append(sql_param)

    function_body, indent_unit = extract_function_body(func)
    indented_body = indent(function_body, " " * indent_unit)

    func_comment = docstring_info.description.replace("'", '"')

    sql_body = assemble_sql_body(
        catalog,
        schema,
        func_name,
        sql_params,
        sql_return_type,
        func_comment,
        indented_body,
        replace,
    )

    return sql_body


def validate_return_type(func_name: str, type_hints: dict[str, Any]) -> str:
    """Validates and returns the SQL return type for the function."""
    if "return" not in type_hints:
        raise ValueError(
            f"Return type for function '{func_name}' is not defined. Please provide a return type."
        )

    return_type_hint = type_hints["return"]
    try:
        sql_return_type = validate_type_hint(return_type_hint)
    except ValueError as e:
        base_msg = f"Error in return type for function '{func_name}': {return_type_hint}."
        origin = get_origin(return_type_hint)
        args = get_args(return_type_hint)

        if (origin in (list, tuple, dict) and not args) or (
            return_type_hint in (list, tuple, dict)
        ):
            base_msg += (
                " Please define the inner types, e.g., List[int], Tuple[str, int], Dict[str, int]."
            )
        elif origin is Union or return_type_hint is Union:
            base_msg += " Union types are not supported in return types."
        elif return_type_hint is Any:
            base_msg += " 'Any' type is not supported. Please specify a concrete return type."
        else:
            base_msg += f" Unsupported return type: {return_type_hint}."

        raise ValueError(base_msg) from e
    return sql_return_type
