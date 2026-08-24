import ast
import inspect
import json
import warnings
from dataclasses import dataclass
from textwrap import dedent, fill, indent
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from unitycatalog.ai.core.utils.docstring_utils import DocstringInfo, parse_docstring
from unitycatalog.ai.core.utils.type_utils import (
    SQL_TYPE_TO_PYTHON_TYPE_MAPPING,
    python_type_to_sql_type,
)

FORBIDDEN_PARAMS = ["self", "cls"]
PRIMARY_INDENT = " " * 4
WRAPPED_INDENT = " " * 6


class FunctionBodyExtractor(ast.NodeVisitor):
    """
    AST NodeVisitor class to extract the body of a function.

    This class traverses the abstract syntax tree (AST) of a Python function
    to extract its body as a dedented string and determine the indentation unit used.

    Attributes:
        func_name (str): The name of the function to extract.
        source_code (str): The source code of the function.
        function_body (str): The extracted function body.
        indent_unit (int): The number of spaces used for indentation.
        found (bool): Flag indicating whether the function has been found.
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


@dataclass
class FunctionMetadata:
    """
    Dataclass to store metadata of a Python function.

    Attributes:
        func_name: The name of the function.
        signature: The signature of the function.
        type_hints: Type hints of the function parameters and return type.
        sql_return_type: The SQL return type corresponding to the function's return type.
        base_return_type_name: The base name of the SQL return type.
        docstring_info: Parsed information from the function's docstring.
        parameters: List of parameter information dictionaries containing name, type, and nullable metadata.
        function_body: The extracted body of the function.
        indent_unit: The number of spaces used for indentation in the function body.
    """

    func_name: str
    signature: inspect.Signature
    type_hints: Dict[str, Any]
    sql_return_type: str
    base_return_type_name: str
    docstring_info: DocstringInfo
    parameters: List[Dict[str, Any]]
    function_body: str
    indent_unit: int


@dataclass
class SQLMetadata:
    """
    Dataclass to store the sql body generation metadata.

    Attributes:
        sql_params: List of SQL parameter definitions.
        func_comment: Comment describing the function.
        indented_body: The indented body of the Python function.
    """

    sql_params: List[str]
    func_comment: str
    indented_body: str


def extract_function_body(func: Callable[..., Any]) -> tuple[str, int]:
    """
    Extracts the body of a function as a string without the signature or docstring,
    dedents the code, and returns the indentation unit used in the function.

    Args:
        func: The function from which to extract the body.

    Returns:
        A tuple containing the dedented function body and the indentation unit.

    """
    source_lines, _ = inspect.getsourcelines(func)
    dedented_source = dedent("".join(source_lines))
    func_name = func.__name__

    extractor = FunctionBodyExtractor(func_name, dedented_source)
    parsed_source = ast.parse(dedented_source)
    extractor.visit(parsed_source)

    return extractor.function_body, extractor.indent_unit


def extract_wrapped_functions(functions: list[Callable[..., Any]]) -> str:
    """
    Extracts the contents of callables that will be wrapped without modification or metadata extraction
    for the purposes of in-lining their definitions into a wrapper function that will be registered to UC.

    Args:
        functions: List of callables to extract.
    Returns:
        The extracted code as a string, prepared for in-lining into the primary function's body.
    """

    if not all(callable(func) for func in functions):
        raise ValueError(
            "Unable to process functions. Ensure that all functions are valid callables."
        )

    sources = []
    for func in functions:
        src, _ = inspect.getsourcelines(func)
        sources.append(dedent("".join(src)))
    return "\n".join(sources)


def validate_type_hint(hint: Any) -> str:
    """
    Validates and returns the SQL type for a given type hint.

    Handles `typing.Optional` by extracting the underlying type and ensures that
    `typing.Any` is not used.

    Args:
        hint: The type hint to validate.

    Returns:
        The corresponding SQL type.
    """
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
    """
    Formats the default value for SQL.

    Args:
        default (Any): The default value to format.

    Returns:
        str: The formatted default value suitable for SQL.

    Examples:
        >>> format_default_value(None)
        'NULL'
        >>> format_default_value("default")
        "'default'"
        >>> format_default_value(42)
        '42'
    """

    if default is None:
        return "NULL"
    elif isinstance(default, str):
        return f"'{default}'"
    else:
        return str(default)


def is_collection_type(type_hint: Any) -> bool:
    """
    Checks if the type hint represents a collection type (list, tuple, dict).

    Args:
        type_hint: The type hint to check.

    Returns:
        True if the type hint is a collection type, False otherwise.
    """

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

    Note:
        This function is required for compatibility with Python 3.9 and lower
        due to the inability to use isinstance with subscripted generic types.
        It can be removed and type hint checks can be done directly with
        isinstance(default, type_hint) when the minimum supported Python version is 3.10+.

    Args:
        default (Any): The default value to validate.
        type_hint (Any): The type hint to validate against.

    Returns:
        bool: True if the default value is valid, False otherwise.
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
    """
    Processes a single parameter and returns its SQL definition.

    This function validates the parameter's type hint, handles default values,
    and formats the SQL parameter string accordingly.

    Args:
        param_name: The name of the parameter.
        param: The parameter object.
        type_hints: The type hints dictionary for the function.
        docstring_info: Parsed docstring information.

    Returns:
        The SQL definition string for the parameter.
    """

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


def construct_dependency_statement(
    dependencies: Optional[list[str]] = None, environment_version: str = "None"
) -> str:
    """
    Constructs the dependency statement for the SQL function.

    Args:
        dependencies: An optional list of PyPI dependencies for the function to utilize in the execution environment
        environment_version: The version of the environment to use for the function. Defaults to "None".

    Returns:
        str: The constructed dependency statement.
    """
    if dependencies:
        dependencies_json = json.dumps(dependencies)
        return f"\nENVIRONMENT (dependencies = '{dependencies_json}', environment_version = '{environment_version}')"
    elif environment_version != "None":
        return f"\nENVIRONMENT (environment_version = '{environment_version}')"
    else:
        return ""


def assemble_sql_body(
    catalog: str,
    schema: str,
    func_name: str,
    sql_params: list[str],
    sql_return_type: str,
    func_comment: str,
    indented_body: str,
    replace: bool,
    dependencies: Optional[list[str]] = None,
    environment_version: str = "None",
) -> str:
    """
    Assembles the final SQL function body.

    Constructs the SQL statement for creating or replacing a function in Unity Catalog.

    Args:
        catalog: The catalog name.
        schema: The schema name.
        func_name: The name of the function.
        sql_params: List of SQL parameter definitions.
        sql_return_type: The SQL return type of the function.
        func_comment: Comment describing the function.
        indented_body: The indented body of the Python function.
        replace: Whether to include the 'OR REPLACE' clause.
        dependencies: An optional list of PyPI dependencies for the function to utilize in the execution environment
        environment_version: The version of the environment to use for the function. Defaults to "None".

    Returns:
        The assembled SQL function creation statement.
    """

    replace_command = "CREATE OR REPLACE" if replace else "CREATE"
    """Assembles the final SQL function body."""
    sql_params_str = ", ".join(sql_params)
    environment_str = construct_dependency_statement(dependencies, environment_version)
    sql_body = f"""
{replace_command} FUNCTION `{catalog}`.`{schema}`.`{func_name}`({sql_params_str})
RETURNS {sql_return_type}
LANGUAGE PYTHON{environment_str}
COMMENT '{func_comment}'
AS $$
{indented_body}
$$;
    """
    return sql_body


def extract_function_metadata(func: Callable[..., Any]) -> FunctionMetadata:
    """
    Extracts metadata from a Python function necessary for SQL function generation.

    This includes the function's signature, type hints, docstring information,
    parameters, and the function body.

    Args:
        func: The Python function to extract metadata from.

    Returns:
        FunctionMetadata: An object containing all extracted metadata.
    """

    func = unwrap_function(func)
    func_name = func.__name__
    signature = inspect.signature(func)
    type_hints = get_type_hints(func)

    sql_return_type = validate_return_type(func_name, type_hints)

    base_return_type_name = sql_return_type.split("<", 1)[0]

    docstring = inspect.getdoc(func)
    if not docstring:
        raise ValueError(f"Function '{func_name}' must have a docstring with a description.")
    docstring_info = parse_docstring(docstring)

    params_in_signature = set(signature.parameters.keys()) - set(FORBIDDEN_PARAMS)
    check_docstring_signature_consistency(docstring_info.params, params_in_signature, func_name)

    parameters = []
    position = 0
    for param_name, param in signature.parameters.items():
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

        base_type_name = sql_type.split("<", 1)[0]

        param_comment = docstring_info.params.get(param_name)
        if param_comment:
            param_comment = param_comment.replace("'", '"')

        parameter_default = None
        if param.default is not inspect.Parameter.empty:
            if is_collection_type(param_hint):
                raise ValueError(
                    f"Parameter '{param_name}' of type '{param_hint}' cannot have a default value."
                )
            if not is_valid_default_value(param.default, param_hint):
                raise ValueError(
                    f"Default value for parameter '{param_name}' does not match the type hint '{param_hint}'."
                )
            parameter_default = format_default_value(param.default)

        param_info = {
            "name": param_name,
            "sql_type": sql_type,
            "base_type_name": base_type_name,
            "comment": param_comment,
            "parameter_default": parameter_default,
            "param": param,  # Original inspect.Parameter object
            "position": position,
        }
        parameters.append(param_info)
        position += 1

    function_body, indent_unit = extract_function_body(func)

    return FunctionMetadata(
        func_name=func_name,
        signature=signature,
        type_hints=type_hints,
        sql_return_type=sql_return_type,
        base_return_type_name=base_return_type_name,
        docstring_info=docstring_info,
        parameters=parameters,
        function_body=function_body,
        indent_unit=indent_unit,
    )


def generate_sql_function_body(
    func: Callable[..., Any],
    catalog: str,
    schema: str,
    replace: bool = False,
    dependencies: Optional[list[str]] = None,
    environment_version: str = "None",
) -> str:
    """
    Generate SQL body for creating the function in Unity Catalog.

    Args:
        func: The Python callable function to convert into a UDF.
        catalog: The catalog name.
        schema: The schema name.
        replace: Whether to replace the function if it already exists. Defaults to False.
        dependencies: An optional list of PyPI dependencies for the function to utilize in the execution environment
        environment_version: The version of the environment to use for the function. Defaults to "None".

    Returns:
        SQL statement for creating the UDF.
    """

    metadata = extract_function_metadata(func)

    sql_metadata = create_sql_metadata(metadata)

    sql_body = assemble_sql_body(
        catalog,
        schema,
        metadata.func_name,
        sql_metadata.sql_params,
        metadata.sql_return_type,
        sql_metadata.func_comment,
        sql_metadata.indented_body,
        replace,
        dependencies,
        environment_version,
    )

    return sql_body


def generate_wrapped_sql_function_body(
    primary_func: Callable[..., Any],
    functions: list[Callable[..., Any]],
    catalog: str,
    schema: str,
    replace: bool = False,
    dependencies: Optional[list[str]] = None,
    environment_version: str = "None",
) -> str:
    """
    Generate SQL body for creating the function in Unity Catalog.

    Args:
        primary_func: The primary Python callable function to convert into a UDF.
        functions: List of additional functions to be wrapped.
        catalog: The catalog name.
        schema: The schema name.
        replace: Whether to include the 'OR REPLACE' clause.
        dependencies: An optional list of PyPI dependencies for the function to utilize in the execution environment
        environment_version: The version of the environment to use for the function. Defaults to "None".

    Returns:
        SQL statement for creating the UDF.
    """

    wrapped_function_content = extract_wrapped_functions(functions)

    primary_metadata = extract_function_metadata(primary_func)

    # Update the primary function's body with the wrapped functions in-lined into the top of the body
    primary_metadata.function_body = (
        wrapped_function_content + "\n" + primary_metadata.function_body
    )

    sql_metadata = create_sql_metadata(primary_metadata)

    sql_body = assemble_sql_body(
        catalog,
        schema,
        primary_metadata.func_name,
        sql_metadata.sql_params,
        primary_metadata.sql_return_type,
        sql_metadata.func_comment,
        sql_metadata.indented_body,
        replace,
        dependencies,
        environment_version,
    )

    return sql_body


def create_sql_metadata(function_metadata: FunctionMetadata) -> SQLMetadata:
    """
    Create SQL body for creating the function in Unity Catalog.

    Args:
        function_metadata: The metadata of the function.

    Returns:
        SQLMetadata object containing the metadata for sql body creation.
    """
    sql_params = []
    for param_info in function_metadata.parameters:
        sql_param_parts = [param_info["name"], param_info["sql_type"]]
        if param_info["parameter_default"] is not None:
            sql_param_parts.append(f"DEFAULT {param_info['parameter_default']}")
        if param_info["comment"]:
            sql_param_parts.append(f"COMMENT '{param_info['comment']}'")
        sql_param = " ".join(sql_param_parts)
        sql_params.append(sql_param)

    indented_body = indent(function_metadata.function_body, " " * function_metadata.indent_unit)

    func_comment = function_metadata.docstring_info.description.replace("'", '"')

    return SQLMetadata(sql_params, func_comment, indented_body)


def validate_return_type(func_name: str, type_hints: dict[str, Any]) -> str:
    """
    Validates and returns the SQL return type for the function.

    Ensures that the return type is properly specified and supported for SQL conversion.

    Args:
        func_name: The name of the function.
        type_hints: The type hints of the function.

    Returns:
        The corresponding SQL return type.
    """

    if "return" not in type_hints:
        raise ValueError(
            f"Return type for function '{func_name}' is not defined. Please provide a return type."
        )

    return_type_hint = type_hints["return"]
    if return_type_hint is None or return_type_hint is type(None):
        return "NULL"
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


def check_docstring_signature_consistency(
    doc_params: Optional[dict[str, str]], signature_params: Set[str], func_name: str
) -> None:
    """
    Checks for inconsistencies between docstring parameters and function signature parameters.
    Issues warnings if there are mismatches.

    Args:
        doc_params (Optional[dict[str, str]]): Parameters documented in the docstring.
        signature_params (Set[str]): Parameters present in the function signature.
        func_name (str): The name of the function being checked.

    Returns:
        None
    """
    params_in_doc = set(doc_params.keys() or {})

    if extra_in_doc := params_in_doc - signature_params:
        warnings.warn(
            f"In function '{func_name}': The following parameters are documented in the docstring but not present in the function signature: {', '.join(sorted(extra_in_doc))}",
            UserWarning,
            stacklevel=2,
        )

    if extra_in_signature := signature_params - params_in_doc:
        warnings.warn(
            f"In function '{func_name}': The following parameters are present in the function signature but not documented in the docstring: {', '.join(sorted(extra_in_signature))}",
            UserWarning,
            stacklevel=2,
        )

    if doc_params and not signature_params:
        warnings.warn(
            f"In function '{func_name}': Docstring defines parameters, but the function has no parameters in its signature.",
            UserWarning,
            stacklevel=2,
        )

    if not doc_params and signature_params:
        warnings.warn(
            f"In function '{func_name}': Function has parameters in its signature, but the docstring does not document any parameters.",
            UserWarning,
            stacklevel=2,
        )


def _split_generic_types(s: str) -> list[str]:
    """
    Split a generic type string into its components, ignoring commas inside nested generics.
    For example, "STRING, ARRAY<STRING>, MAP<STRING, INT>" becomes
    ["STRING", "ARRAY<STRING>", "MAP<STRING, INT>"].
    """
    parts = []
    current_chars = []
    depth = 0
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1

        if ch == "," and depth == 0:
            parts.append("".join(current_chars).strip())
            current_chars = []
        else:
            current_chars.append(ch)
    if current_chars:
        parts.append("".join(current_chars).strip())
    return parts


def _parse_sql_data_type(type_str: str) -> str:
    """
    Convert a full SQL type string (e.g. "MAP<STRING, ARRAY<STRING>>")
    into a Python type annotation string (e.g. "dict[str, list[str]]").
    """
    type_str = type_str.strip()

    if "<" not in type_str:
        if mapped := SQL_TYPE_TO_PYTHON_TYPE_MAPPING.get(type_str.upper()):
            return mapped[0].__name__ if isinstance(mapped, tuple) else mapped.__name__
        else:
            return type_str.lower()

    outer, inner = type_str.split("<", 1)
    outer = outer.strip().upper()
    inner = inner.rsplit(">", 1)[0].strip()

    parts = _split_generic_types(inner)

    # recurse
    parsed_parts = [_parse_sql_data_type(part) for part in parts]

    # NB: For Python tuples, we map both tuple and list to the SQL ARRAY type.
    # Since we can't disambiguate between them in the SQL type string, we assume that
    # the collection container type is list for simplicity's sake, which is the first
    # mapping entry in SQL_TYPE_TO_PYTHON_TYPE_MAPPING for ARRAY types.
    if outer == "ARRAY":
        return f"list[{parsed_parts[0]}]"
    elif outer == "MAP":
        return f"dict[{parsed_parts[0]}, {parsed_parts[1]}]"
    elif outer == "STRUCT":
        return "dict"
    else:
        return f"{outer.lower()}[{', '.join(parsed_parts)}]"


def _reconstruct_docstring(function_info: "FunctionInfo", max_width: int = 100) -> str:
    """
    Reconstruct a Google-style docstring from a FunctionInfo object.

    This function rebuilds:
      - The function description from function_info.comment.
      - An "Args:" section from each parameter's comment.
      - A "Returns:" section using the return type (from full_data_type).

    Each section is word-wrapped to a maximum width of `max_width` characters,
    wrapping only at full words.

    Returns:
        A string representing the reconstructed docstring (with triple quotes and proper indentation).
    """
    doc_lines = []

    # Overall function description.
    if hasattr(function_info, "comment") and function_info.comment:
        description = function_info.comment.strip()
        wrapped_description = fill(description, width=max_width)
        doc_lines.append(wrapped_description)

    # Build the Args: section.
    if function_info.input_params and function_info.input_params.parameters:
        doc_lines.append("")  # Blank line.
        doc_lines.append("Args:")
        for param in function_info.input_params.parameters:
            param_comment = (getattr(param, "comment", None) or "").strip()
            arg_comment = f": {param_comment}" if param_comment else ""
            arg_line = param.name + arg_comment
            wrapped_arg = fill(
                arg_line,
                width=max_width,
                initial_indent=PRIMARY_INDENT,
                subsequent_indent=WRAPPED_INDENT + PRIMARY_INDENT,
            )
            doc_lines.append(wrapped_arg)

    # Build the Returns: section.
    return_type_str = _parse_sql_data_type(function_info.full_data_type)
    doc_lines.append("")
    doc_lines.append("Returns:")
    wrapped_return = fill(
        return_type_str,
        width=max_width,
        initial_indent=PRIMARY_INDENT,
        subsequent_indent=WRAPPED_INDENT + PRIMARY_INDENT,
    )
    doc_lines.append(wrapped_return)

    if not doc_lines:
        return ""

    indented_doc = "\n".join(PRIMARY_INDENT + line for line in doc_lines)
    return f'    """\n{indented_doc}\n    """\n'


def _parse_routine_definition(routine_definition: str) -> str:
    """
    Normalize the indentation of a function body to a standard 4-space indent.

    This function processes a raw routine definition string by:
      1. Dedenting the entire block (using textwrap.dedent) to remove any common
         leading whitespace.
      2. Splitting the dedented text into lines.
      3. Determining the "standard" indent (i.e. the minimum nonzero indent among lines).
      4. Calculating an indent factor as int(4 / indent_standard) to scale the original
         indentation to a 4-space baseline.
      5. For each nonempty line, computing the new indent level as:
             new_indent = (original indent) * indent_factor
         and reformatting the line with that indent (using line.strip() to remove trailing
         whitespace).
      6. Finally, reindenting the entire block by an additional 4 spaces (for overall nesting).

    NB:
      - The methodology assumes that the smallest nonzero indent found in the block
        represents the "unit" of indentation in the original text.
      - If such an indent is found, the indent_factor scales all indentations so that the
        unit becomes 4 spaces. For example, if the original unit is 2 spaces, the factor will
        be 2 (i.e. 4 / 2), converting a 2-space indent to 4 spaces and a 6-space indent to 12 spaces.
      - If no nonzero indent is found (i.e. baseline is 0), the indent_factor defaults to 1,
        and every nonempty line is given a fixed indent of 4 spaces.
      - This approach preserves the relative indentation of nested blocks while standardizing
        the overall appearance of the function body.

    Args:
        routine_definition (str): The raw routine definition string with its original indentation.

    Returns:
        str: The normalized function body, where each nonempty line is reindented based on a 4-space standard.
    """
    dedented = dedent(routine_definition).rstrip()
    lines = dedented.splitlines()

    indent_standard = min(
        [len(line) - len(line.lstrip()) for line in lines if len(line.lstrip()) < len(line)],
        default=0,
    )
    indent_factor = 4 // indent_standard if indent_standard > 0 else 1
    normalized_lines = []
    for line in lines:
        if line.strip():
            indent_level = (len(line) - len(line.lstrip())) * indent_factor
            normalized_lines.append(" " * indent_level + line.strip())
        else:
            normalized_lines.append("")
    return indent("\n".join(normalized_lines), PRIMARY_INDENT)


def dynamically_construct_python_function(
    function_info: "FunctionInfo",
) -> str:
    """
    Construct a Python function from the given FunctionInfo object.

    Note: This function will not recreate the original docstring of the function.

    Args:
        function_info: The FunctionInfo object containing the function metadata.

    Returns:
        The re-constructed function definition as a string.
    """
    if isinstance(function_info.routine_body, str):
        function_type = function_info.routine_body
    else:
        function_type = function_info.routine_body.value

    if function_type != "EXTERNAL":
        raise NotImplementedError(
            f"routine_body {function_type} is not supported. Only 'EXTERNAL' Python body extraction is supported."
        )

    param_names = []
    if function_info.input_params and function_info.input_params.parameters:
        for param in function_info.input_params.parameters:
            param_type = _parse_sql_data_type(param.type_text)
            param_names.append(f"{param.name}: {param_type}")
    return_type = _parse_sql_data_type(function_info.full_data_type)
    function_head = f"{function_info.name}({', '.join(param_names)}) -> {return_type}"
    func_def = f"def {function_head}:\n"

    docstring = _reconstruct_docstring(function_info)
    if docstring:
        func_def += docstring

    adjusted_body = _parse_routine_definition(function_info.routine_definition)
    func_def += adjusted_body + "\n"

    return func_def


def get_callable_definition(function_info: "FunctionInfo") -> str:
    """
    Construct a Python function from the given FunctionInfo without docstring, comments, or types.
    This funciton is purely used for local or sandboxed function execution encapsulated within a call to
    `execute_function` and is not intended to be used for retrieving a callable definition.
    Use `get_python_callable` instead if you want the original callable definition.

    Args:
        function_info: The FunctionInfo object containing the function metadata.

    Returns:
        The minimal re-constructed function definition.
    """

    if isinstance(function_info.routine_body, str):
        function_type = function_info.routine_body
    else:
        function_type = function_info.routine_body.value

    if function_type != "EXTERNAL":
        raise NotImplementedError(
            f"routine_body {function_type} is not supported. Only 'EXTERNAL' Python body extraction is supported."
        )
    param_names = []
    if function_info.input_params and function_info.input_params.parameters:
        param_names = [param.name for param in function_info.input_params.parameters]
    function_head = f"{function_info.name}({', '.join(param_names)})"
    func_def = f"def {function_head}:\n"
    for line in function_info.routine_definition.split("\n"):
        func_def += f"    {line}\n"

    return func_def
