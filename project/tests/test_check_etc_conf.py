#!/usr/bin/env python3
"""Check project/scripts/check_etc_conf.sh against a throwaway git repository."""

import subprocess
import tempfile
from pathlib import Path


UC_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = UC_ROOT / "project" / "scripts" / "check_etc_conf.sh"


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
                   cwd=repo, check=True, capture_output=True)


def check(repo: Path) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", str(SCRIPT)], cwd=repo, capture_output=True, text=True)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)
        git(repo, "init", "-q")
        (repo / "etc" / "conf").mkdir(parents=True)
        (repo / "etc" / "conf" / "server.properties").write_text("server.env=dev\n")
        # Like the real .gitignore: the generated files are ignored in etc/conf.
        (repo / ".gitignore").write_text("etc/conf/*.txt\n")
        git(repo, "add", ".")
        git(repo, "commit", "-q", "-m", "init")

        result = check(repo)
        assert result.returncode == 0, f"a clean tree must pass:\n{result.stdout}{result.stderr}"

        # Each case must fail and name the file: ignored, unignored in a module, hidden, edited.
        cases = {
            "etc/conf/token.txt": "!! etc/conf/token.txt",
            "examples/cli/etc/conf/certs.json": "?? examples/cli/etc/conf/certs.json",
            "etc/conf/.hidden": "?? etc/conf/.hidden",
            "etc/conf/server.properties": " M etc/conf/server.properties",
        }
        for path, expected in cases.items():
            file = repo / path
            original = file.read_text() if file.exists() else None
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("written by a test\n")
            result = check(repo)
            assert result.returncode == 1 and expected in result.stdout, (
                f"{path}: expected a failure listing {expected!r}, got exit {result.returncode}:\n"
                f"{result.stdout}{result.stderr}")
            if original is None:
                file.unlink()
            else:
                file.write_text(original)

        result = check(repo)
        assert result.returncode == 0, f"the restored tree must pass:\n{result.stdout}"
    print("PASS: check_etc_conf.sh fails on new, ignored, hidden and edited files in any etc/conf")


if __name__ == "__main__":
    main()
