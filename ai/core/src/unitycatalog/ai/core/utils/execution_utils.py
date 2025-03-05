import ast


class ExecutionMode:
    """
    A configuration class for execution modes that validates the provided mode
    against allowed values based on the client type. It also exposes the current
    configuration state.

    Args:
        mode: The execution mode to set.
        client_type: The type of client to set the execution mode for.

    """

    _allowed_modes = {
        "databricks": {"serverless", "local"},
        "unitycatalog": {"sandbox", "local"},
    }

    def __init__(self, mode: str, client_type: str):
        self.client_type = client_type.lower()
        self.mode = self._validate_mode(mode)

    def _validate_mode(self, mode: str) -> str:
        mode = mode.lower()
        allowed = self._allowed_modes.get(self.client_type)
        if allowed is None:
            raise ValueError(f"Unknown client type: {self.client_type}")
        if mode not in allowed:
            raise ValueError(
                f"Execution mode '{mode}' is not valid for client '{self.client_type}'. "
                f"Allowed values are: {', '.join(allowed)}."
            )
        return mode

    def __str__(self) -> str:
        return self.mode


def load_function_from_string(
    func_def_str: str, register_function: bool = True, namespace: dict = None
) -> callable:
    """
    Convert a function definition string into a callable using the ast module.

    This function parses the string into an AST, finds the first function definition,
    compiles the AST into a code object, executes it in a temporary namespace,
    and optionally registers the function in the global namespace.

    Example usage:

    With default global registration (does not work in interactive environments):
    ```python
    func_str = '''
    def multiply_numbers(a: int, b: int) -> int:
        \"\"\"
        Multiplies two numbers.
            Args:
                a (int): first number.
                b (int): second number.
        \"\"\"
        return a * b
    '''
    function_def = load_function_from_string(func_str, register_function=True)
    multiply_numbers(a=2, b=4)  # returns 8
    ```

    With default global registration in an interactive environment (i.e. Jupyter):
    ```python
    func_str = '''
    def multiply_numbers(a: int, b: int) -> int:
        \"\"\"
        Multiplies two numbers.
            Args:
                a (int): first number.
                b (int): second number.
        \"\"\"
        return a * b
    '''

    function_def = load_function_from_string(func_str, register_function=True)

    globals()["multiply_numbers"] = (
        function_def  # directly applying to globals outside of the callable is required for Jupyter
    )

    multiply_numbers(a=2, b=4)  # returns 8
    ```

    With custom namespace:
    ```python
    from types import SimpleNamespace

    func_str = '''
    def multiply_numbers_with_constant(a: int, b: int) -> int:
        \"\"\"
        Multiplies two numbers with a constant.
            Args:
                a (int): first number.
                b (int): second number.
        \"\"\"
        return a * b * c
    '''

    scoped_namespace = {
        "__builtins__": __builtins__,
        "c": 42,
    }
    c = 100  # this will not be used in the function
    load_function_from_string(func_str, register_function=True, namespace=scoped_namespace)

    scoped_ns = SimpleNamespace(**scoped_namespace)

    scoped_ns.multiply_numbers_with_constant(a=2, b=3)  # returns 252
    ```

    With no registration and direct usage of the aliased function object:
    ```python
    func_str = '''
    def multiply_numbers(a: int, b: int) -> int:
        \"\"\"
        Multiplies two numbers.
            Args:
                a (int): first number.
                b (int): second number.
        \"\"\"
        return a * b
    '''

    function_def = load_function_from_string(func_str, register_function=False)

    function_def(a=3, b=4)  # returns 12
    ```

    Args:
        func_def_str: A string containing a valid Python function definition.
        register_function: If True, registers the function in either the specified
            `namespace` entry or, if `namespace` is not specified, the global namespace.
            If False, the function is not registered and is only accessible via the
            returned callable function object.
        namespace: Optional dictionary to execute the function in a specific namespace.

    Returns:
        callable: The function object created from the string.
    """

    namespace = namespace if namespace is not None else globals()

    function_error = "Function definition not found in the provided string."

    try:
        module_ast = ast.parse(func_def_str)
    except SyntaxError as e:
        raise ValueError(function_error) from e

    func_def_node = None
    for node in module_ast.body:
        if isinstance(node, ast.FunctionDef):
            func_def_node = node
            break
    if func_def_node is None:
        raise ValueError(function_error)

    func_name = func_def_node.name
    code_obj = compile(module_ast, filename="<ast>", mode="exec")
    temp_namespace = {}
    exec(code_obj, namespace, temp_namespace)
    func_obj = temp_namespace[func_name]

    if register_function:
        namespace[func_name] = func_obj
    return func_obj
