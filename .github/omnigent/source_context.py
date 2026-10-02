"""Bounded read-only source access for CI review agents."""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath
from typing import Iterator, Literal

Repository = Literal["pr", "delta"]

_ROOT_ENV = {
    "pr": "PR_SOURCE_ROOT",
    "delta": "DELTA_SOURCE_ROOT",
}
_MAX_FILE_BYTES = 2_000_000
_MAX_OUTPUT_CHARS = 40_000
_MAX_SCANNED_BYTES = 64_000_000
_MAX_SCANNED_FILES = 20_000


def _source_root(repository: Repository) -> Path:
    env_name = _ROOT_ENV.get(repository)
    if env_name is None:
        raise ValueError("repository must be 'pr' or 'delta'")
    raw_root = os.environ.get(env_name)
    if not raw_root:
        raise ValueError(f"{env_name} is not configured")
    root = Path(raw_root).resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"{repository} source root is not a directory")
    return root


def _resolve(repository: Repository, path: str, *, allow_root: bool = False) -> tuple[Path, Path]:
    root = _source_root(repository)
    relative = PurePosixPath(path)
    if relative.is_absolute() or ".." in relative.parts or ".git" in relative.parts:
        raise ValueError("path must stay within the selected source tree")
    if not allow_root and (not path or path == "."):
        raise ValueError("path must name a file")

    target = root.joinpath(*relative.parts).resolve(strict=True) if path else root
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise ValueError("path resolves outside the selected source tree") from exc
    return root, target


def _bounded(text: str) -> str:
    if len(text) <= _MAX_OUTPUT_CHARS:
        return text
    return text[:_MAX_OUTPUT_CHARS] + "\n[Output truncated by the source reader.]"


def _read_text(path: Path) -> str:
    size = path.stat().st_size
    if size > _MAX_FILE_BYTES:
        raise ValueError(f"file exceeds the {_MAX_FILE_BYTES}-byte read limit")
    data = path.read_bytes()
    if b"\x00" in data:
        raise ValueError("binary files are not supported")
    return data.decode("utf-8", errors="replace")


def _iter_files(root: Path, target: Path) -> Iterator[Path]:
    candidates = [target] if target.is_file() else target.rglob("*")
    for candidate in candidates:
        relative = candidate.relative_to(root)
        if ".git" in relative.parts or candidate.is_symlink():
            continue
        try:
            resolved = candidate.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, RuntimeError, ValueError):
            continue
        if resolved.is_file():
            yield resolved


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
    try:
        if start_line < 1 or not 1 <= line_count <= 1000:
            raise ValueError("start_line must be positive and line_count must be 1 through 1000")
        _, target = _resolve(repository, path)
        if not target.is_file():
            raise ValueError("path is not a file")
        lines = _read_text(target).splitlines()
        selected = lines[start_line - 1 : start_line - 1 + line_count]
        numbered = [f"{start_line + index}\t{line}" for index, line in enumerate(selected)]
        return _bounded("\n".join(numbered))
    except (OSError, TypeError, ValueError) as exc:
        return f"Error: {exc}"


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
    try:
        if not 1 <= max_entries <= 1000:
            raise ValueError("max_entries must be 1 through 1000")
        root, target = _resolve(repository, path, allow_root=True)
        entries = []
        for candidate in _iter_files(root, target):
            entries.append(candidate.relative_to(root).as_posix())
            if len(entries) >= max_entries:
                break
        return _bounded("\n".join(entries))
    except (OSError, TypeError, ValueError) as exc:
        return f"Error: {exc}"


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
    try:
        if not query or len(query) > 200:
            raise ValueError("query must contain 1 through 200 characters")
        if not 1 <= max_results <= 500:
            raise ValueError("max_results must be 1 through 500")

        root, target = _resolve(repository, path, allow_root=True)
        results = []
        scanned_bytes = 0
        scanned_files = 0
        for candidate in _iter_files(root, target):
            if scanned_files >= _MAX_SCANNED_FILES or scanned_bytes >= _MAX_SCANNED_BYTES:
                results.append("[Search limit reached. Narrow the path and retry.]")
                break
            size = candidate.stat().st_size
            scanned_files += 1
            scanned_bytes += size
            if size > _MAX_FILE_BYTES:
                continue
            data = candidate.read_bytes()
            if b"\x00" in data:
                continue
            relative = candidate.relative_to(root).as_posix()
            lines = data.decode("utf-8", errors="replace").splitlines()
            for line_number, line in enumerate(lines, 1):
                if query in line:
                    results.append(f"{relative}:{line_number}: {line[:500]}")
                    if len(results) >= max_results:
                        return _bounded("\n".join(results))
        return _bounded("\n".join(results))
    except (OSError, TypeError, ValueError) as exc:
        return f"Error: {exc}"
