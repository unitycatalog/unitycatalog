"""Omnigent entrypoint for bounded source-file reads."""

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
def read_source_file(
    repository: Repository,
    path: str,
    start_line: int = 1,
    line_count: int = 400,
) -> str:
    """Read bounded, line-numbered text from a source file.

    Args:
        repository: Read from the exact PR checkout or read-only Delta reference.
        path: Repository-relative POSIX path to a text file.
        start_line: First line to return, using one-based numbering.
        line_count: Number of lines to return, from 1 through 1000.
    """
    return _implementation().read_source_file(repository, path, start_line, line_count)
