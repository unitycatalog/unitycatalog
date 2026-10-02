"""Tests for mechanically binding sub-agent dispatches to PR context."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_policy():
    path = Path(__file__).parents[1] / "review_context_policy.py"
    spec = importlib.util.spec_from_file_location("review_context_policy", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dispatch(input_text: str) -> dict:
    return {
        "type": "tool_call",
        "target": "sys_session_send",
        "data": {
            "name": "sys_session_send",
            "arguments": {
                "agent": "architecture-reviewer",
                "title": "architecture-review",
                "args": {"input": input_text, "purpose": "review"},
            },
        },
    }


def test_dispatch_includes_canonical_context_even_when_input_is_placeholder(tmp_path: Path) -> None:
    policy = _load_policy()
    context_path = tmp_path / "review-context.txt"
    context_path.write_text("Head SHA: abc123\n\n```diff\n+actual change\n```\n")

    result = policy.inject_review_context(context_path=str(context_path))(_dispatch("review it"))

    assert result["result"] == "ALLOW"
    child_input = result["data"]["args"]["input"]
    assert child_input.startswith("review it\n\n## Canonical PR context")
    assert "Head SHA: abc123" in child_input
    assert "+actual change" in child_input


def test_dispatch_fails_closed_without_canonical_context(tmp_path: Path) -> None:
    policy = _load_policy()
    evaluate = policy.inject_review_context(context_path=str(tmp_path / "missing.txt"))

    assert evaluate(_dispatch("review it"))["result"] == "DENY"


def test_non_dispatch_tool_is_unchanged(tmp_path: Path) -> None:
    policy = _load_policy()
    evaluate = policy.inject_review_context(context_path=str(tmp_path / "missing.txt"))

    assert evaluate({"type": "tool_call", "target": "sys_read_inbox"}) == {
        "result": "ALLOW"
    }
