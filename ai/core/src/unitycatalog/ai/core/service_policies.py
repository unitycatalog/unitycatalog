import ast
import inspect
from textwrap import dedent
from typing import Any, Callable, get_type_hints

from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.callable_utils import FORBIDDEN_PARAMS

SERVICE_POLICY_PARAM_COUNT = 2
SERVICE_POLICY_MIN_ENVIRONMENT_VERSION = 6


def _decision_payload(result: str, reason: str = "") -> dict:
    """
    Build the decision envelope that Unity Catalog expects from a service policy.

    Args:
        result: The decision result, either "ALLOW" or "DENY".
        reason: An explanation for the decision.

    Returns:
        dict: The decision envelope.
    """
    return {"decision": {"result": result, "reason": reason}}


def _decision(result: str, reason: str = "") -> "VariantVal":  # noqa: F821
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

    return VariantVal.parseJson(json.dumps(_decision_payload(result, reason)))


def allow(reason: str = "") -> "VariantVal":  # noqa: F821
    """
    Build an ALLOW decision for a service policy function.

    Args:
        reason: An optional explanation for the decision.

    Returns:
        VariantVal: The decision to return from a service policy function.
    """
    return _decision("ALLOW", reason)


def deny(reason: str = "") -> "VariantVal":  # noqa: F821
    """
    Build a DENY decision for a service policy function.

    Args:
        reason: An explanation for the denial.

    Returns:
        VariantVal: The decision to return from a service policy function.
    """
    return _decision("DENY", reason)


SERVICE_POLICY_HELPERS = (_decision_payload, _decision, allow, deny)
INLINED_HELPER_NAMES = frozenset(helper.__name__ for helper in SERVICE_POLICY_HELPERS)


class _ServicePolicyBindingVisitor(ast.NodeVisitor):
    def __init__(self):
        self.names: set[str] = set()

    def visit_Name(self, node: ast.Name):
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.names.add(node.id)

    def visit_FunctionDef(self, node: ast.FunctionDef):
        self.names.add(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_ClassDef(self, node: ast.ClassDef):
        self.names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda):
        return

    def visit_ListComp(self, node: ast.ListComp):
        return

    visit_SetComp = visit_ListComp
    visit_DictComp = visit_ListComp
    visit_GeneratorExp = visit_ListComp

    def visit_Import(self, node: ast.Import):
        self.names.update(
            alias.asname or alias.name.split(".", maxsplit=1)[0] for alias in node.names
        )

    def visit_ImportFrom(self, node: ast.ImportFrom):
        self.names.update(alias.asname or alias.name for alias in node.names)


def _get_service_policy_bindings(func: Callable[..., Any]) -> set[str]:
    source = dedent(inspect.getsource(func))
    function = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func.__name__
    )
    visitor = _ServicePolicyBindingVisitor()
    for statement in function.body:
        visitor.visit(statement)
    return visitor.names


def _validate_service_policy_environment_version(environment_version: object) -> None:
    """
    Validate that an environment version can run a service policy.

    Service policies require environment version 6 or later. Named environment channels are
    validated by Unity Catalog because their version cannot be determined locally.

    Args:
        environment_version: The environment version declared for the service policy.

    Raises:
        ValueError: If the version is absent or is a number below the required minimum.
    """
    if environment_version is None or environment_version == "None":
        raise ValueError(
            "Service policies require an environment_version of at least "
            f"'{SERVICE_POLICY_MIN_ENVIRONMENT_VERSION}', but {environment_version!r} was given."
        )

    if not isinstance(environment_version, str):
        raise ValueError(
            "Service policies require environment_version to be a string of at least "
            f"'{SERVICE_POLICY_MIN_ENVIRONMENT_VERSION}', but {environment_version!r} was given."
        )

    major_version = environment_version.split("-", 1)[0]
    if major_version.isdigit() and int(major_version) < SERVICE_POLICY_MIN_ENVIRONMENT_VERSION:
        raise ValueError(
            "Service policies require an environment_version of at least "
            f"'{SERVICE_POLICY_MIN_ENVIRONMENT_VERSION}', but {environment_version!r} was given."
        )


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
            "with generated helpers "
            f"({sorted(INLINED_HELPER_NAMES)}). Rename them."
        )

    if collisions := sorted(_get_service_policy_bindings(func) & INLINED_HELPER_NAMES):
        raise ValueError(
            f"Service policy {func.__name__} binds names {collisions} that shadow generated helper "
            f"names ({sorted(INLINED_HELPER_NAMES)}). Rename them."
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
