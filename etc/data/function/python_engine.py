import sys
import json
import logging

# Configure logging
logging.basicConfig(filename='python_engine.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_function(func_name, routine_body, params_str, args_str):
    try:
        # Split the parameter names and argument values
        param_names = params_str.split(', ')
        args = json.loads(args_str)

        # Log the inputs
        logging.info(f"Executing function '{func_name}' with parameters {param_names} and arguments {args}")

        # Define the function dynamically
        func_def = f"def {func_name}({', '.join(param_names)}):\n"
        for line in routine_body.split('\\n'):
            func_def += f"    {line}\n"
        #print(func_def)
        exec(func_def, globals())

        # Execute the function with the provided arguments
        result = eval(f"{func_name}(*args)")

        # Log the result
        logging.info(f"Result of function '{func_name}': {result}")

        return result
    except Exception as e:
        logging.error(f"Error executing function '{func_name}': {str(e)}")
        return str(e)

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python_engine.py <func_name> <routine_body> <params> <args>")
        sys.exit(1)

    func_name = sys.argv[1]
    routine_body = sys.argv[2]
    params_str = sys.argv[3]
    args_str = sys.argv[4]

    result = execute_function(func_name, routine_body, params_str, args_str)
    print(json.dumps(result))