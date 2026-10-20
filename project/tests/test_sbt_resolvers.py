#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path


UC_ROOT = Path(__file__).resolve().parents[2]
MAVEN_LOCAL = ".m2/repository"


def effective_resolvers(use_maven_local: bool) -> str:
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
    return result.stdout + result.stderr


def main() -> None:
    default_resolvers = effective_resolvers(use_maven_local=False)
    assert (
        "maven-local" not in default_resolvers.lower()
        and MAVEN_LOCAL not in default_resolvers
    ), (
        "Maven local must not be in build/sbt's default resolver chain"
    )

    opted_in_resolvers = effective_resolvers(use_maven_local=True)
    assert MAVEN_LOCAL in opted_in_resolvers, (
        "USE_MAVEN_LOCAL=true must add ~/.m2/repository to build/sbt's resolver chain"
    )
    print("PASS: build/sbt excludes Maven local by default and adds it when opted in")


if __name__ == "__main__":
    main()
