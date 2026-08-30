import inspect
from typing import Any, Callable, get_type_hints

from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.callable_utils import FORBIDDEN_PARAMS

SERVICE_POLICY_PARAM_COUNT = 2
# The helpers in-lined at the top of a generated policy body. A parameter sharing one of these
# names is rebound by the in-lined `def`, silently discarding the argument.
INLINED_HELPER_NAMES = frozenset({"decision_payload", "decision", "allow", "deny"})


def decision_payload(result: str, reason: str = "") -> dict:
    """
    Build the decision envelope that Unity Catalog expects from a service policy.

    Args:
        result: The decision result, either "ALLOW" or "DENY".
        reason: An explanation for the decision.

    Returns:
        dict: The decision envelope.
    """
    return {"decision": {"result": result, "reason": reason}}


def decision(result: str, reason: str = "") -> "VariantVal":  # noqa: F821
    """
    Build a service policy decision.

    Unity Catalog requires a VARIANT-returning Python UDF to return a `VariantVal`, so the
    envelope is serialised to JSON and parsed back into one.

    Args:
        result: The decision result, either "ALLOW" or "DENY".
        reason: An explanation for the decision.

    Returns:
        VariantVal: The decision to return from a service policy function.
    """
    import json

    from pyspark.sql.types import VariantVal

    return VariantVal.parseJson(json.dumps(decision_payload(result, reason)))


def allow(reason: str = "") -> "VariantVal":  # noqa: F821
    """
    Build an ALLOW decision for a service policy function.

    Args:
        reason: An optional explanation for the decision.

    Returns:
        VariantVal: The decision to return from a service policy function.
    """
    return decision("ALLOW", reason)


def deny(reason: str = "") -> "VariantVal":  # noqa: F821
    """
    Build a DENY decision for a service policy function.

    Args:
        reason: An explanation for the denial.

    Returns:
        VariantVal: The decision to return from a service policy function.
    """
    return decision("DENY", reason)


def resolve_service_policy_parameters(func: Callable[..., Any]) -> list[str]:
    """
    Validate that a callable has the signature Unity Catalog requires of a service policy, and
    return the names of its event and configuration parameters.

    A service policy takes an event and a configuration, both VARIANT, and returns a VARIANT
    decision. The two parameters are bound positionally, so their names are free; the first is
    the event and the second is the configuration.

    Args:
        func: The callable to validate.

    Returns:
        The names of the event and configuration parameters, in order.

    Raises:
        ValueError: If the callable does not take exactly two `Variant` parameters, or does not
            return a `Variant`.
    """
    parameters = [
        name for name in inspect.signature(func).parameters if name not in FORBIDDEN_PARAMS
    ]
    if len(parameters) != SERVICE_POLICY_PARAM_COUNT:
        raise ValueError(
            f"A service policy must take exactly {SERVICE_POLICY_PARAM_COUNT} parameters, an "
            f"event and a configuration, but {func.__name__} takes {len(parameters)}: "
            f"{parameters}."
        )

    if collisions := [name for name in parameters if name in INLINED_HELPER_NAMES]:
        raise ValueError(
            f"Service policy {func.__name__} has parameters named {collisions}, which collide "
            "with the decision helpers in-lined into the policy body "
            f"({sorted(INLINED_HELPER_NAMES)}). Rename them."
        )

    type_hints = get_type_hints(func)
    for position, name in enumerate(parameters):
        if type_hints.get(name) is not Variant:
            role = "event" if position == 0 else "configuration"
            raise ValueError(
                f"The {role} parameter {name!r} of service policy {func.__name__} must be "
                f"annotated as Variant, but is annotated as {type_hints.get(name)!r}."
            )

    if type_hints.get("return") is not Variant:
        raise ValueError(
            f"A service policy must return a Variant, but {func.__name__} is annotated as "
            f"returning {type_hints.get('return')!r}."
        )

    return parameters


def build_variant_conversion_preamble(parameters: list[str]) -> str:
    """
    Build the statements that convert a policy's VARIANT arguments into Python values.

    Unity Catalog passes a VARIANT argument to a Python UDF as a `VariantVal`, which has no
    mapping-like behaviour of its own. Converting up front lets a policy body index and iterate
    its event and configuration as ordinary dictionaries. The conversion is guarded so that a
    value that already arrives as a Python object passes through untouched.

    Args:
        parameters: The names of the parameters to convert.

    Returns:
        The conversion statements, one per parameter.
    """
    return "\n".join(
        f'{name} = {name}.toPython() if hasattr({name}, "toPython") else {name}'
        for name in parameters
    )
