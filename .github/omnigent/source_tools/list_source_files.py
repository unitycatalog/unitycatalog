"""Omnigent entrypoint for bounded source-tree listings."""

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
def list_source_files(
    repository: Repository,
    path: str = "",
    max_entries: int = 500,
) -> str:
    """List files recursively below a source-tree path.

    Args:
        repository: List the exact PR checkout or read-only Delta reference.
        path: Optional repository-relative file or directory path.
        max_entries: Maximum paths to return, from 1 through 1000.
    """
    return _implementation().list_source_files(repository, path, max_entries)
