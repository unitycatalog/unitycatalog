import asyncio
import resource
import traceback
import builtins
from multiprocessing import Process, Queue

DISALLOWED_MODULES = {
    'os', 'sys', 'subprocess', 'ctypes', 'socket',
    'importlib', 'pickle', 'marshal', 'shutil', 'pathlib'
}

def limit_resources(cpu_time_limit, memory_limit):
    """
    Limit CPU and memory usage.
    
    Parameters:
        cpu_time_limit: Maximum CPU time in seconds.
        memory_limit: Maximum memory in MB.
    """
    try:
        # Set the CPU time limit (in seconds)
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_time_limit, cpu_time_limit))
    except Exception as e:
        print("Warning: unable to set RLIMIT_CPU:", e)
    
    try:
        # Set the virtual memory limit (convert MB to bytes)
        memory_bytes = memory_limit * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except Exception as e:
        print("Warning: unable to set RLIMIT_AS:", e)

def disable_unwanted_imports():
    """
    Override the built-in __import__ and open function to block
    potentially dangerous modules and file access.
    """
    original_import = builtins.__import__
    
    def restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in DISALLOWED_MODULES:
            raise ImportError(f"Import of module '{name}' is restricted")
        return original_import(name, globals, locals, fromlist, level)
    
    builtins.__import__ = restricted_import

    # NB: To limit file system access from within a UC callable, as an added 
    # layer of security, override the built-in open function.
    def disabled_open(*args, **kwargs):
        raise ImportError("The open function is disabled in this sandbox")
    builtins.open = disabled_open

def sandboxed_wrapper(q, func, params, cpu_time_limit, memory_limit):
    """
    Execute the provided callable in a sandboxed environment.
    
    Applies resource limits and restricts dangerous module imports.
    The function `func` is called with keyword arguments from `params`.
    The result or any exception's full stack trace is placed on a queue.
    """
    try:
        # Apply CPU and memory limits.
        limit_resources(cpu_time_limit, memory_limit)
        # Restrict dangerous imports and file system access.
        disable_unwanted_imports()
        # Execute the function with the provided keyword parameters.
        result = func(**params)
        q.put((True, result))
    except Exception:
        tb = traceback.format_exc()
        q.put((False, tb))

async def async_run_in_sandbox(func, params, *, timeout=2, cpu_time_limit=1, memory_limit=50):
    """
    Executes a Python callable in a sandboxed environment using multiprocessing.
    Specific core Python modules are restricted to prevent unwanted behavior.
    The function is executed with a timeout and resource limits for CPU and memory.
    The function's result or any exception's full stack trace is returned.
    
    Parameters:
        func: The callable to execute.
        params: A dictionary of keyword arguments to pass to the function.
        timeout: The maximum time (in seconds) to wait for the function to complete.
        cpu_time_limit: The maximum CPU time allowed for the function (in seconds).
        memory_limit: The maximum memory allowed for the function (in MB).
      
    Returns:
        A tuple (success, result). If success is False, result contains the error stack trace.
    """
    q = Queue()
    p = Process(target=sandboxed_wrapper, args=(q, func, params, cpu_time_limit, memory_limit))
    p.start()
    
    loop = asyncio.get_event_loop()
    try:
        # Asynchronously wait for the process to complete.
        await asyncio.wait_for(loop.run_in_executor(None, p.join), timeout)
    except asyncio.TimeoutError:
        p.terminate()  # Terminate if execution exceeds the timeout.
        await loop.run_in_executor(None, p.join)
        return False, "The function execution has timed out. Please configure your client to increase the timeout."  ## TODO: create helper exceptions here! 
    
    if not q.empty():
        return q.get()
    else:
        return False, "No output from process"
