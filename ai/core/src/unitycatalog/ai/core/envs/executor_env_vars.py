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
    "Maximum memory limit for the executor measured in MB.",
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
