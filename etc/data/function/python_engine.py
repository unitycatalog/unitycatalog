import ast
import sys
import json
import logging

# Configure logging
logging.basicConfig(filename='python_engine.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_body(routine_body):
    # A body without a real newline separates its lines with the two characters backslash and n,
    # as in the CLI examples (--def "c=a*b\nreturn c").
    if '\n' not in routine_body.rstrip('\r\n'):
        routine_body = routine_body.replace('\\n', '\n')
    try:
        return ast.parse(routine_body, filename='<routine_definition>').body
    except IndentationError as original:
        # A body indented as a whole parses inside an indented block, with its strings and
        # comments kept as written. Any statement outside that block means the body is malformed.
        try:
            wrapped = ast.parse("if 1:\n" + routine_body, filename='<routine_definition>')
        except SyntaxError:
            raise original from None
        block = wrapped.body[0] if len(wrapped.body) == 1 else None
        if not isinstance(block, ast.If) or block.orelse:
            raise original from None
        return block.body

def define_function(func_name, routine_body, param_names):
    func_def = ast.parse(f"def {func_name}({', '.join(param_names)}):\n    pass")
    body = parse_body(routine_body)
    if not body:
        raise SyntaxError("the routine definition has no statements")
    func_def.body[0].body = body
    exec(compile(func_def, '<routine_definition>', 'exec'), globals())

def execute_function(func_name, routine_body, params_str, args_str):
    try:
        # Split the parameter names and argument values
        param_names = params_str.split(', ')
        args = json.loads(args_str)

        # Log the inputs
        logging.info(f"Executing function '{func_name}' with parameters {param_names} and arguments {args}")

        define_function(func_name, routine_body, param_names)

        # Execute the function with the provided arguments
        result = eval(f"{func_name}(*args)")

        # Log the result
        logging.info(f"Result of function '{func_name}': {result}")

        return result
    except Exception as e:
        logging.error(f"Error executing function '{func_name}': {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python_engine.py <func_name> <routine_body> <params> <args>")
        sys.exit(1)

    func_name = sys.argv[1]
    routine_body = sys.argv[2]
    params_str = sys.argv[3]
    args_str = sys.argv[4]

    try:
        print(json.dumps(execute_function(func_name, routine_body, params_str, args_str)))
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
