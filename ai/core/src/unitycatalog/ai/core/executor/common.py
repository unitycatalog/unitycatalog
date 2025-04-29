DISALLOWED_MODULES = {
    "sys",
    "subprocess",
    "ctypes",
    "socket",
    "importlib",
    "pickle",
    "marshal",
    "shutil",
}
MB_CONVERSION = 1024 * 1024

TIMEOUT_ERROR_MESSAGE = (
    "The function execution has timed out and has been canceled due to excessive resource consumption.\n"
    "There are two timeout conditions to consider:\n"
    "\t1. You can increase the CPU execution timeout by setting "
    "the environment variable EXECUTOR_MAX_CPU_TIME_LIMIT. The default value is 10 seconds.\n"
    "\t2. You can increase the wall-clock time limit by setting the environment variable "
    "EXECUTOR_TIMEOUT. The default value is 20 seconds.\n"
)

NO_OUTPUT_MESSAGE = (
    "The function execution has completed, but no output was produced. "
    "This may indicate that the function did not return a value or that an error occurred "
    "without being captured. Check the function implementation by calling the `get_function_source` client API. "
    "Alternatively, to directly fetch a usable callable for direct local execution, utilize the `get_function_as_callable` client API."
)

OPEN_DISALLOWED_MESSAGE = (
    "The use of the 'open' function is restricted within the local sandbox executor for safety reasons. "
    "This is to prevent the execution of potentially dangerous code or access to sensitive system resources. "
    "If you need to read or write files within your function, utilize a different execution mode in your function client."
)

IMPORT_DISALLOWED_MESSAGE = (
    "You may not use this module within the local sandbox executor for safety reasons. "
    "This is to prevent the execution of potentially dangerous code or access to sensitive system resources. "
    "Set a different execution mode in your function client in order to execute this function."
)


TERMINATED_MESSAGE_TEMPLATE = (
    "The function execution has been terminated with a signal {signal_num}. "
    "This likely indicates that the function exceeded its resource limits "
    "(CPU execution time or exceeded its virtual memory cgroup allocation). "
    "You can adjust the following environment variables to increase the limits if "
    "you are certain that your function is behaving as expected:\n"
    "\t1. EXECUTOR_MAX_CPU_TIME_LIMIT: Maximum CPU execution time limit for the executor "
    "measured in CPU seconds (total CPU execution time, not wall-clock time).\n"
    "\t2. EXECUTOR_MAX_MEMORY_LIMIT: Maximum memory limit for the executor measured in MB. "
    "The default value is 100 MB. Note that this restriction is only applicable to Linux environments "
    "and has no functional use in Mac OSX or Windows.\n"
)

IMPORT_DISALLOWED_MESSAGE_TEMPLATE = (
    "The import of module '{module_name}' is restricted. {import_disallowed_message}"
)
