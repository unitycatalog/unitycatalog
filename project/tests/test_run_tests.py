#!/usr/bin/env python3
"""Check dev/run-tests.py: argument checks, failure summaries, and a parallel run."""

import contextlib
import importlib.util
import io
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from xml.etree import ElementTree


UC_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = UC_ROOT / "dev" / "run-tests.py"
DURATIONS = UC_ROOT / "target" / "test-durations.csv"
SEED_ROW = "zz.SeedSuite,1.0"


def load_script():
    spec = importlib.util.spec_from_file_location("run_tests", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_arguments(run_tests) -> None:
    for argv in (["-p", "0"], ["-p", "x"], ["--jvm-heap", "1.5g"], ["--jvm-heap", "512"],
                 ["--sbt-heap", "2gb"]):
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                run_tests.parse_args(argv)
        except SystemExit as e:
            assert e.code == 2, f"{argv} should be a usage error, got exit {e.code}"
        else:
            raise AssertionError(f"{argv} should be rejected")
    args = run_tests.parse_args(["-p", "3", "--jvm-heap", "512m", "server/test"])
    assert (args.parallelism, args.jvm_heap, args.commands) == (3, "512m", ["server/test"])
    assert run_tests.parse_size("512m") == 512 << 20


def check_failure_description(run_tests) -> None:
    case = ElementTree.fromstring(
        '<testcase name="t()"><failure message="boom&#10;expected: 3">'
        "java.lang.AssertionError: boom\n\tat a.B.t(B.java:7)\n</failure></testcase>")
    description = run_tests.describe_failure(case, "a.B")
    assert description == "a.B#t(): boom expected: 3 (at a.B.t(B.java:7))", description


def check_failure_summary(run_tests) -> None:
    failure = "a.B#t(): boom (at a.B.t(B.java:7))"
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        run_tests.print_summary([("a.B", 1.0, 2, [failure])], 0, 1, "/tmp/x.log", ["137"])
    for expected in ("1 failed", failure, "exited early (exit code 137)", "sbt exited with code 1"):
        assert expected in out.getvalue(), f"{expected!r} missing from:\n{out.getvalue()}"


def check_sbt_errors_shown(run_tests) -> None:
    # sbt failed before any suite reported, e.g. a mistyped task: its errors must reach the console.
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp) / "run.log"
        log.write_text("[info] loading\n[error] Not a valid key: tset\n[info] done\n")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            run_tests.print_summary([], 0, 1, str(log))
    assert "[error] Not a valid key: tset" in out.getvalue(), out.getvalue()


def check_durations_file(run_tests) -> None:
    saved = run_tests.DURATIONS_FILE
    with tempfile.TemporaryDirectory() as tmp:
        durations = Path(tmp) / "test-durations.csv"
        # A row cut off mid-write must be skipped, not stop the merge.
        durations.write_text("a.Old,3.0\nb.Trunc,\n")
        run_tests.DURATIONS_FILE = str(durations)
        try:
            run_tests.update_durations([("c.New", 2.0, 1, [])])
        finally:
            run_tests.DURATIONS_FILE = saved
        assert durations.read_text().splitlines() == ["a.Old,3.0", "c.New,2.0"], (
            durations.read_text())
        assert [p.name for p in Path(tmp).iterdir()] == ["test-durations.csv"], "temp file left"


def check_cgroup_memory(run_tests) -> None:
    gib = 1 << 30
    saved = run_tests.PROC_CGROUP, run_tests.CGROUP_MEMORY
    with tempfile.TemporaryDirectory() as root:
        files = {
            # The limit is on the parent: 8 GiB, 6 GiB used, of which 4 GiB is reclaimable cache.
            "a/memory.max": str(8 * gib),
            "a/memory.current": str(6 * gib),
            "a/memory.stat": f"anon 1\ninactive_file {4 * gib}\n",
            "a/b/memory.max": "max",
            "a/b/memory.current": str(6 * gib),
            "proc_cgroup": "0::/a/b\n",
        }
        for name, content in files.items():
            (Path(root) / name).parent.mkdir(parents=True, exist_ok=True)
            (Path(root) / name).write_text(content)
        run_tests.PROC_CGROUP = str(Path(root) / "proc_cgroup")
        run_tests.CGROUP_MEMORY = {"v2": (root, "memory.max", "memory.current", "inactive_file")}
        try:
            left = run_tests.cgroup_memory_left()
        finally:
            run_tests.PROC_CGROUP, run_tests.CGROUP_MEMORY = saved
    assert left == 6 * gib, f"expected 6 GiB left under the parent's limit, got {left}"


def check_concurrency_limit() -> None:
    command = [str(UC_ROOT / "build" / "sbt"), "-no-colors", "-batch",
               "show Global / concurrentRestrictions"]
    result = subprocess.run(command, cwd=UC_ROOT, env={**os.environ, "TEST_PARALLELISM": "2"},
                            capture_output=True, text=True, check=False)
    output = result.stdout + result.stderr
    assert result.returncode == 0, f"{command!r} exited with {result.returncode}\n{output}"
    for rule in ("Limit forked-test-group to 2", "Limit assembly to 1"):
        assert rule in output, f"{rule!r} missing from:\n{output}"


def check_parallel_run() -> None:
    # `clean` checks that the log and durations survive it: the seeded row is not a client suite, so
    # only a kept durations file still has it. An sbt heap below build/sbt's default -Xms of 1000m
    # checks that sbt still starts.
    DURATIONS.parent.mkdir(parents=True, exist_ok=True)
    with DURATIONS.open("a") as f:
        f.write(SEED_ROW + "\n")
    command = [sys.executable, str(SCRIPT), "-p", "2", "--sbt-heap", "900m", "--jvm-heap", "1g",
               "clean", "client/test"]
    try:
        result = subprocess.run(command, cwd=UC_ROOT, capture_output=True, text=True, check=False)
        rows = DURATIONS.read_text().splitlines()
    finally:
        if DURATIONS.is_file():
            kept = [row for row in DURATIONS.read_text().splitlines() if row != SEED_ROW]
            DURATIONS.write_text("".join(row + "\n" for row in kept))
    output = result.stdout + result.stderr
    assert result.returncode == 0, f"{command!r} exited with {result.returncode}\n{output}"

    log = Path(re.search(r"Full log: (\S+)", output).group(1))
    assert log.is_file(), f"the log is missing after the run: {log}"
    log_text = log.read_text()
    assert re.search(r"unitycatalog-client: \d+ suites in 2 test JVMs", log_text), (
        f"the client suites were not split across 2 test JVMs; see {log}")
    group_size = re.compile(r"unitycatalog-client test-group-\d+: (\d+) suites")
    sizes = [int(n) for n in group_size.findall(log_text)]
    assert len(sizes) == 2 and min(sizes) > 0, f"suites not spread across both groups: {sizes}"
    summary = re.search(r"(\d+) suites, (\d+) tests, (\d+) failed", output)
    assert summary and int(summary.group(1)) > 1 and summary.group(3) == "0", output
    assert SEED_ROW in rows, f"{DURATIONS} lost its earlier rows across `clean`"
    assert any(row.startswith("io.unitycatalog.client.") for row in rows), (
        f"no client suites in {DURATIONS}")


def main() -> None:
    run_tests = load_script()
    check_arguments(run_tests)
    check_failure_description(run_tests)
    check_failure_summary(run_tests)
    check_sbt_errors_shown(run_tests)
    check_durations_file(run_tests)
    check_cgroup_memory(run_tests)
    check_concurrency_limit()
    check_parallel_run()
    print("PASS: dev/run-tests.py validates its arguments and runs client/test in 2 test JVMs")


if __name__ == "__main__":
    main()
