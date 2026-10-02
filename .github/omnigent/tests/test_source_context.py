"""Tests for the AI review source-context tools."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path


def _load_source_context():
    path = Path(__file__).parents[1] / "source_context.py"
    spec = importlib.util.spec_from_file_location("source_context", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_read_list_and_search_are_confined_to_source_roots(tmp_path: Path) -> None:
    source_context = _load_source_context()
    pr_root = tmp_path / "pr"
    delta_root = tmp_path / "delta"
    pr_root.mkdir()
    delta_root.mkdir()
    (pr_root / "src").mkdir()
    (pr_root / "src" / "lib.rs").write_text("fn visible() {}\nfn target() {}\n")
    (delta_root / "PROTOCOL.md").write_text("# Protocol\nMUST retain context\n")
    secret = tmp_path / "secret.txt"
    secret.write_text("do not expose\n")
    (pr_root / "outside").symlink_to(secret)
    secret_dir = tmp_path / "secret-dir"
    secret_dir.mkdir()
    (secret_dir / "hidden.txt").write_text("directory symlink secret\n")
    (pr_root / "outside-dir").symlink_to(secret_dir, target_is_directory=True)

    os.environ["PR_SOURCE_ROOT"] = str(pr_root)
    os.environ["DELTA_SOURCE_ROOT"] = str(delta_root)

    assert "2\tfn target() {}" in source_context.read_source_file("pr", "src/lib.rs", 2, 1)
    assert source_context.list_source_files("pr") == "src/lib.rs"
    assert source_context.search_source_code("delta", "MUST") == (
        "PROTOCOL.md:2: MUST retain context"
    )
    assert source_context.read_source_file("pr", "../secret.txt").startswith("Error:")
    assert source_context.read_source_file("pr", "outside").startswith("Error:")
    assert "outside-dir" not in source_context.list_source_files("pr")
    assert source_context.search_source_code("pr", "directory symlink secret") == ""
