import importlib.util

import pytest

from unitycatalog.ai.core.service_policies import (
    allow,
    build_variant_conversion_preamble,
    decision,
    decision_payload,
    deny,
    resolve_service_policy_parameters,
)
from unitycatalog.ai.core.types import Variant
from unitycatalog.ai.core.utils.callable_utils import generate_wrapped_sql_function_body


def valid_policy(event: Variant, config: Variant) -> Variant:
    """
    Allow everything.

    Args:
        event (Variant): The request event.
        config (Variant): The policy configuration.

    Returns:
        Variant: The policy decision.
    """
    return allow()


def test_resolve_service_policy_parameters_returns_names_in_order():
    assert resolve_service_policy_parameters(valid_policy) == ["event", "config"]


def test_parameter_names_are_not_constrained():
    def renamed(payload: Variant, settings: Variant) -> Variant:
        """
        Allow everything.

        Args:
            payload (Variant): The request event.
            settings (Variant): The policy configuration.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    assert resolve_service_policy_parameters(renamed) == ["payload", "settings"]


def test_too_few_parameters():
    def one_param(event: Variant) -> Variant:
        """
        Allow everything.

        Args:
            event (Variant): The request event.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    with pytest.raises(ValueError, match="must take exactly 2 parameters"):
        resolve_service_policy_parameters(one_param)


def test_too_many_parameters():
    def three_params(event: Variant, config: Variant, extra: Variant) -> Variant:
        """
        Allow everything.

        Args:
            event (Variant): The request event.
            config (Variant): The policy configuration.
            extra (Variant): An extra parameter.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    with pytest.raises(ValueError, match="must take exactly 2 parameters"):
        resolve_service_policy_parameters(three_params)


def test_event_parameter_must_be_variant():
    def bad_event(event: str, config: Variant) -> Variant:
        """
        Allow everything.

        Args:
            event (str): The request event.
            config (Variant): The policy configuration.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    with pytest.raises(ValueError, match="event parameter 'event'.*must be annotated as Variant"):
        resolve_service_policy_parameters(bad_event)


def test_configuration_parameter_must_be_variant():
    def bad_config(event: Variant, config: dict) -> Variant:
        """
        Allow everything.

        Args:
            event (Variant): The request event.
            config (dict): The policy configuration.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    with pytest.raises(
        ValueError, match="configuration parameter 'config'.*must be annotated as Variant"
    ):
        resolve_service_policy_parameters(bad_config)


def test_return_must_be_variant():
    def bad_return(event: Variant, config: Variant) -> str:
        """
        Allow everything.

        Args:
            event (Variant): The request event.
            config (Variant): The policy configuration.

        Returns:
            str: Not a decision.
        """
        return "ALLOW"

    with pytest.raises(ValueError, match="must return a Variant"):
        resolve_service_policy_parameters(bad_return)


def test_unannotated_parameters_are_rejected():
    def unannotated(event, config) -> Variant:
        """
        Allow everything.

        Args:
            event: The request event.
            config: The policy configuration.

        Returns:
            Variant: The policy decision.
        """
        return allow()

    with pytest.raises(ValueError, match="must be annotated as Variant"):
        resolve_service_policy_parameters(unannotated)


@pytest.mark.parametrize(
    ("parameters", "expected"),
    [
        ([], ""),
        (["event"], 'event = event.toPython() if hasattr(event, "toPython") else event'),
        (
            ["event", "config"],
            'event = event.toPython() if hasattr(event, "toPython") else event\n'
            'config = config.toPython() if hasattr(config, "toPython") else config',
        ),
    ],
)
def test_build_variant_conversion_preamble(parameters, expected):
    assert build_variant_conversion_preamble(parameters) == expected


def test_generated_policy_sql_is_self_contained():
    sql_body = generate_wrapped_sql_function_body(
        primary_func=valid_policy,
        functions=[decision_payload, decision, allow, deny],
        catalog="main",
        schema="policies",
        replace=True,
        body_preamble=build_variant_conversion_preamble(["event", "config"]),
    )

    assert "(event VARIANT COMMENT 'The request event.'" in sql_body
    assert "config VARIANT COMMENT 'The policy configuration.')" in sql_body
    assert "RETURNS VARIANT" in sql_body
    # The decision builders must be in-lined so that the body does not depend on this library
    # being installed in the execution environment.
    assert "def decision_payload(" in sql_body
    assert "def decision(" in sql_body
    assert "def allow(" in sql_body
    assert "def deny(" in sql_body
    # The VARIANT arguments are converted before the policy body runs.
    assert 'event = event.toPython() if hasattr(event, "toPython") else event' in sql_body
    assert sql_body.index("def deny(") < sql_body.index("event = event.toPython()")


def test_inlined_helpers_only_reference_builtins_in_annotations():
    # The in-lined source is executed inside the UDF body, where the annotations are evaluated at
    # definition time. Anything but a builtin or a string annotation would raise NameError there.
    sql_body = generate_wrapped_sql_function_body(
        primary_func=valid_policy,
        functions=[decision_payload, decision, allow, deny],
        catalog="main",
        schema="policies",
        replace=True,
    )

    for signature in [
        'def decision(result: str, reason: str = "") -> "VariantVal":',
        'def allow(reason: str = "") -> "VariantVal":',
        'def deny(reason: str = "") -> "VariantVal":',
    ]:
        assert signature in sql_body


@pytest.mark.parametrize("shadowed", ["decision_payload", "decision", "allow", "deny"])
def test_parameters_may_not_shadow_the_inlined_helpers(shadowed, tmp_path):
    # The in-lined helper `def`s rebind a parameter of the same name, so the argument is silently
    # discarded and the policy fails at evaluation time instead of at creation time.
    module = tmp_path / "policy_module.py"
    module.write_text(
        "from unitycatalog.ai.core.types import Variant\n\n\n"
        f"def policy({shadowed}: Variant, config: Variant) -> Variant:\n"
        '    """\n    Allow everything.\n\n    Args:\n'
        f"        {shadowed} (Variant): The request event.\n"
        "        config (Variant): The policy configuration.\n\n"
        "    Returns:\n        Variant: The policy decision.\n"
        '    """\n    return None\n'
    )
    spec = importlib.util.spec_from_file_location("policy_module", module)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)

    with pytest.raises(ValueError, match="collide with the decision helpers"):
        resolve_service_policy_parameters(loaded.policy)


def test_decision_payload_is_the_documented_envelope():
    # Runs under every CI job; the VariantVal wrapping below requires pyspark, which the general
    # `dev` lock does not install.
    assert decision_payload("ALLOW") == {"decision": {"result": "ALLOW", "reason": ""}}
    assert decision_payload("DENY", "nope") == {"decision": {"result": "DENY", "reason": "nope"}}


def test_generated_policy_inlines_the_payload_builder():
    sql_body = generate_wrapped_sql_function_body(
        primary_func=valid_policy,
        functions=[decision_payload, decision, allow, deny],
        catalog="main",
        schema="policies",
        replace=True,
    )

    assert "def decision_payload(" in sql_body
    assert sql_body.index("def decision_payload(") < sql_body.index("def decision(")


def test_decision_builders_produce_the_documented_envelope():
    pytest.importorskip("pyspark", reason="VariantVal construction requires pyspark")
    import json

    assert json.loads(allow().toJson()) == {"decision": {"result": "ALLOW", "reason": ""}}
    assert json.loads(deny("nope").toJson()) == {"decision": {"result": "DENY", "reason": "nope"}}
    assert json.loads(decision("ALLOW", "why").toJson()) == {
        "decision": {"result": "ALLOW", "reason": "why"}
    }
