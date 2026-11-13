from unitycatalog.ai.core.envs.base import _EnvironmentVariable
from unitycatalog.ai.core.executor.common import DISALLOWED_MODULES

EXECUTOR_MAX_CPU_TIME_LIMIT = _EnvironmentVariable(
    "EXECUTOR_MAX_CPU_TIME_LIMIT",
    int,
    10,
    "Maximum CPU execution time limit for the executor measured in CPU seconds (total CPU execution time, not wall-clock time).",
)

EXECUTOR_MAX_MEMORY_LIMIT = _EnvironmentVariable(
    "EXECUTOR_MAX_MEMORY_LIMIT",
    int,
    100,
    "Maximum memory limit for the executor measured in MB. In sandbox mode, this is the virtual "
    "memory the function can allocate on top of the address space the forked process inherits. "
    "In the Databricks client's local mode, it caps the whole subprocess.",
)

EXECUTOR_TIMEOUT = _EnvironmentVariable(
    "EXECUTOR_TIMEOUT",
    int,
    20,
    "Maximum timeout for the executor measured in seconds (wall clock time).",
)

EXECUTOR_DISALLOWED_MODULES = _EnvironmentVariable(
    "EXECUTOR_DISALLOWED_MODULES",
    list[str],
    DISALLOWED_MODULES,
    "List of disallowed modules for the executor sandbox execution mode.",
)
