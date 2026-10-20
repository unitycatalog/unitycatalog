#!/usr/bin/env python3
"""Check the effective resolver chains used by the bundled sbt launcher."""

import os
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse


UC_ROOT = Path(__file__).resolve().parents[2]
MAVEN_LOCAL = ".m2/repository"


def resolver_chains(use_maven_local: bool) -> list[list[str]]:
    env = os.environ.copy()
    env.pop("MAVEN_PROXY_URL", None)
    env.pop("MAVEN_LOCAL_REPO", None)
    env.pop("USE_MAVEN_LOCAL", None)
    if use_maven_local:
        env["USE_MAVEN_LOCAL"] = "true"

    result = subprocess.run(
        [str(UC_ROOT / "build/sbt"), "-batch", "show fullResolvers"],
        cwd=UC_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    output_lines = (result.stdout + result.stderr).splitlines()
    chains = []
    for index, line in enumerate(output_lines[:-1]):
        if not re.fullmatch(r"\[info\] (?:[^/]+ / )?fullResolvers", line):
            continue
        task_result = re.fullmatch(r"\[info\]\s+Vector\((.*)\)", output_lines[index + 1])
        assert task_result, f"Expected fullResolvers task result after {line!r}"
        chains.append(split_resolver_entries(task_result.group(1)))
    assert chains, "sbt did not print any fullResolvers task results"
    return chains


def split_resolver_entries(value: str) -> list[str]:
    entries = []
    depth = 0
    start = 0
    for index, character in enumerate(value):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            entries.append(value[start:index].strip())
            start = index + 1
    entries.append(value[start:].strip())
    return entries


def main() -> None:
    default_chains = resolver_chains(use_maven_local=False)
    for chain in default_chains:
        assert all(not entry.startswith("maven-local:") for entry in chain), (
            "Maven local must not be in build/sbt's default resolver chains"
        )
        assert all(MAVEN_LOCAL not in entry for entry in chain), (
            "~/.m2/repository must not be in build/sbt's default resolver chains"
        )

    opted_in_chains = resolver_chains(use_maven_local=True)
    expected_path = (Path.home() / MAVEN_LOCAL).resolve()
    for chain in opted_in_chains:
        last_entry = chain[-1]
        name, separator, uri = last_entry.partition(":")
        assert separator and name == "maven-local" and uri.strip().startswith("file:"), (
            "USE_MAVEN_LOCAL=true must append a file-based maven-local resolver"
        )
        assert Path(urlparse(uri.strip()).path).resolve() == expected_path, (
            "maven-local must point to ~/.m2/repository"
        )
        assert any("https://" in entry for entry in chain[:-1]), (
            "maven-local must follow the configured remote repositories"
        )
    print("PASS: build/sbt excludes Maven local by default and appends it after remote resolvers")


if __name__ == "__main__":
    main()
