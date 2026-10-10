"""Omnigent entrypoint for bounded source-code searches."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Literal

from omnigent_client import tool

Repository = Literal["pr", "delta"]


def _implementation():
    path = Path(__file__).resolve().parents[2] / "lib" / "source_context.py"
    spec = importlib.util.spec_from_file_location("source_context_impl", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load the source-context implementation")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@tool
def search_source_code(
    repository: Repository,
    query: str,
    path: str = "",
    max_results: int = 100,
) -> str:
    """Search source text for a fixed, case-sensitive string.

    Args:
        repository: Search the exact PR checkout or read-only Delta reference.
        query: Fixed text to find; regular expressions are not accepted.
        path: Optional repository-relative file or directory path.
        max_results: Maximum matching lines to return, from 1 through 500.
    """
    return _implementation().search_source_code(repository, query, path, max_results)
