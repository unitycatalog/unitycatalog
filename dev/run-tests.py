#!/usr/bin/env python3
"""
Run the sbt test suites in parallel forked JVMs (see project/TestParallelization.scala).

Usage:
    dev/run-tests.py                                  # same tests as `build/sbt test`
    dev/run-tests.py server/test cli/test             # specific sbt commands
    dev/run-tests.py "server/testOnly *Catalog*"
    dev/run-tests.py -p 8                             # at most 8 concurrent test JVMs
    dev/run-tests.py --jvm-heap 4g                    # max heap of each test JVM
    dev/run-tests.py -v                               # stream the full sbt output

By default the number of test JVMs is half the CPUs, lowered if the available memory cannot fit
that many JVMs of --jvm-heap each.

The full sbt output goes to target/test-logs/. Suite durations from each run are merged into
target/test-durations.csv and used to balance the suites across JVMs on the next run.
Other environment variables pass through to the test JVMs. With UC_TEST_DATABASE, each test JVM
starts its own database container, which the memory estimate does not count; lower -p if needed.
"""

import argparse
import csv
import glob
import os
import re
import subprocess
import sys
import threading
import time
from xml.etree import ElementTree

# Peak resident memory of a JVM relative to its max heap (metaspace, threads, direct buffers).
# Spark test JVMs, the largest, peak at up to 1.8x their heap; the others stay well below.
JVM_FOOTPRINT_PER_HEAP = 1.8
# Memory left for the OS and everything else on the machine.
RESERVED_BYTES = 4 << 30
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DURATIONS_FILE = os.path.join(ROOT, "target", "test-durations.csv")
LOG_DIR = os.path.join(ROOT, "target", "test-logs")
# sbt's line for a test JVM that exited before running all its suites.
JVM_DIED = re.compile(r"sbt\.ForkMain \d+ failed with exit code (\d+)")
SUITE_FINISHED = re.compile(r"Test run finished: (\d+) failed")
# A JVM size for -Xmx, with a unit: 512m, 4g. A bare number would mean bytes.
HEAP_SIZE = re.compile(r"[1-9][0-9]*[kKmMgG]")
# cgroup files holding a container's memory limit and usage (v2, then v1).
CGROUP_MEMORY_FILES = [
    ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
    ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes"),
]
# sbt's own result lines; forked JVM stderr is also logged as [error], so match narrowly.
STREAMED = re.compile(
    r"^\[success\]|^\[error\] (Test .* failed|Failed|Error|\(\S+ / |Total|Compilation failed"
    r"|/\S+\.(java|scala):\d+)")


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return number


def heap_size(value):
    if not HEAP_SIZE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"expected a JVM size such as 512m or 4g, got {value!r}")
    return value


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "commands", nargs="*", default=["test"],
        help="sbt commands to run (default: test)")
    parser.add_argument(
        "-p", "--parallelism", type=positive_int,
        help="max concurrent test JVMs (default: half the CPUs, if memory allows)")
    parser.add_argument(
        "--jvm-heap", default="2g", type=heap_size,
        help="max heap of each test JVM (default: 2g; raise it if a test runs out of memory)")
    parser.add_argument(
        "--sbt-heap", default="2g", type=heap_size,
        help="heap for the sbt JVM itself (default: 2g)")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="stream the full sbt output")
    return parser.parse_args(argv)


def parse_size(size):
    """Parses a JVM size such as 4g or 512m into bytes."""
    units = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}
    unit = size[-1].lower()
    return int(size[:-1]) * units[unit] if unit in units else int(size)


def available_memory():
    """Returns the available memory in bytes: MemAvailable on Linux, else half the physical RAM.

    Inside a container, the result is capped by what is left of the container's memory limit.
    """
    available = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    available = int(line.split()[1]) * 1024
    except OSError:
        pass
    for limit_file, usage_file in CGROUP_MEMORY_FILES:
        try:
            with open(limit_file) as f:
                limit = f.read().strip()
            with open(usage_file) as f:
                usage = int(f.read().strip())
        except (OSError, ValueError):
            continue
        # "max" (v2) or a huge value (v1) means no limit.
        if limit != "max" and int(limit) < 1 << 60:
            available = min(available, int(limit) - usage)
        break
    return available


def memory_limit(args):
    """Returns how many test JVMs fit in the available memory, after sbt and a reserve."""
    free = available_memory() - parse_size(args.sbt_heap) * JVM_FOOTPRINT_PER_HEAP - RESERVED_BYTES
    return max(1, int(free // (parse_size(args.jvm_heap) * JVM_FOOTPRINT_PER_HEAP)))


def choose_parallelism(args):
    cpu_limit = max(1, (os.cpu_count() or 2) // 2)
    mem_limit = memory_limit(args)
    gb = available_memory() / (1 << 30)
    if args.parallelism is None:
        args.parallelism = min(cpu_limit, mem_limit)
        print(f"Using {args.parallelism} test JVMs: {cpu_limit} from half the CPUs, "
              f"{mem_limit} fit in {gb:.0f} GB available memory at --jvm-heap {args.jvm_heap}")
    elif args.parallelism > mem_limit:
        print(f"Warning: {args.parallelism} test JVMs at --jvm-heap {args.jvm_heap} may not fit in "
              f"{gb:.0f} GB available memory (estimated limit {mem_limit})")


def run_sbt(args, log_path):
    """Runs sbt, writing all output to log_path.

    Returns the sbt exit code and the exit codes of test JVMs that died before finishing.
    """
    env = dict(
        os.environ, TEST_PARALLELISM=str(args.parallelism), TEST_JVM_HEAP=args.jvm_heap,
        TEST_DURATIONS_FILE=DURATIONS_FILE)
    # build/sbt passes its own -Xms1000m first, so set -Xms too: a smaller -Xmx alone fails.
    heap = [f"-J-Xms{args.sbt_heap}", f"-J-Xmx{args.sbt_heap}"]
    cmd = [os.path.join(ROOT, "build", "sbt")] + heap + args.commands
    print(f"Running {' '.join(cmd[1:])} with {args.parallelism} test JVMs")
    print(f"Full log: {log_path}")

    finished = [0, 0]  # suites finished, suites with failures
    dead_jvms = []
    start = time.time()
    done = threading.Event()

    def report_progress():
        while not done.wait(30):
            print(f"  [{elapsed(start)}] {finished[0]} suites finished, {finished[1]} failed",
                  flush=True)

    if not args.verbose:
        threading.Thread(target=report_progress, daemon=True).start()
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, universal_newlines=True, errors="replace", bufsize=1)
        for line in proc.stdout:
            log.write(line)
            match = SUITE_FINISHED.search(line)
            if match:
                finished[0] += 1
                finished[1] += int(match.group(1)) > 0
            died = JVM_DIED.search(line)
            if died:
                dead_jvms.append(died.group(1))
            if args.verbose or STREAMED.match(line):
                sys.stdout.write(line)
        returncode = proc.wait()
    done.set()
    return returncode, dead_jvms


def describe_failure(case, suite_name):
    """Returns "Suite#test: message (at first stack frame)" for a failed or errored test case."""
    problem = case.find("failure")
    if problem is None:
        problem = case.find("error")
    message = " ".join((problem.get("message") or problem.get("type") or "").split())
    if len(message) > 300:
        message = message[:300] + "..."
    lines = (line.strip() for line in (problem.text or "").splitlines())
    frames = [line for line in lines if line.startswith("at ")]
    location = f" ({frames[0]})" if frames else ""
    return f"{suite_name}#{case.get('name')}: {message}{location}"


def collect_reports(since):
    """Returns (suite, seconds, tests, failures) for each JUnit XML report written since `since`."""
    results = []
    # Modules live one or two levels deep (server/, connectors/spark/).
    paths = glob.glob(os.path.join(ROOT, "*", "target", "test-reports", "*.xml")) + glob.glob(
        os.path.join(ROOT, "*", "*", "target", "test-reports", "*.xml"))
    for path in paths:
        if os.path.getmtime(path) < since:
            continue
        try:
            suite = ElementTree.parse(path).getroot()
        except ElementTree.ParseError:
            continue
        failed = [
            describe_failure(case, suite.get("name"))
            for case in suite.iter("testcase")
            if case.find("failure") is not None or case.find("error") is not None
        ]
        results.append(
            (suite.get("name"), float(suite.get("time", 0)), int(suite.get("tests", 0)), failed))
    return results


def update_durations(results):
    """Merges this run's suite durations into DURATIONS_FILE. Failing to write only warns."""
    durations = {}
    try:
        if os.path.isfile(DURATIONS_FILE):
            with open(DURATIONS_FILE) as f:
                durations = {row[0]: float(row[1]) for row in csv.reader(f) if len(row) == 2}
        durations.update({suite: seconds for suite, seconds, _, _ in results})
        os.makedirs(os.path.dirname(DURATIONS_FILE), exist_ok=True)
        with open(DURATIONS_FILE, "w", newline="") as f:
            csv.writer(f).writerows(sorted(durations.items(), key=lambda kv: -kv[1]))
    except (OSError, ValueError) as e:
        print(f"Warning: could not update {DURATIONS_FILE}: {e}")


def print_summary(results, start, returncode, log_path, dead_jvms=()):
    failed = [name for _, _, _, names in results for name in names]
    print(f"\n{len(results)} suites, {sum(r[2] for r in results)} tests, "
          f"{len(failed)} failed, wall time {elapsed(start)}")
    print("Slowest suites:")
    for suite, seconds, _, _ in sorted(results, key=lambda r: -r[1])[:10]:
        print(f"  {seconds:7.1f}s  {suite}")
    if failed:
        print("Failed tests:")
        for failure in failed:
            print(f"  {failure}")
        print(f"Full output and stack traces: {log_path}, and each module's target/test-reports/")
    if dead_jvms:
        print(f"{len(dead_jvms)} test JVM(s) exited early (exit code {', '.join(dead_jvms)}), so "
              "their remaining suites did not run and are missing above. Exit code 137 usually "
              "means out of memory: lower -p or raise --jvm-heap.")
    if returncode != 0:
        print(f"sbt exited with code {returncode}; see the log for details")


def conf_stamps():
    """Returns the mtime of each file in etc/conf, which tests should never write."""
    conf = os.path.join(ROOT, "etc", "conf")
    if not os.path.isdir(conf):
        return {}
    return {name: os.path.getmtime(os.path.join(conf, name)) for name in os.listdir(conf)}


def elapsed(start):
    minutes, seconds = divmod(int(time.time() - start), 60)
    return f"{minutes}m{seconds:02d}s"


def main():
    args = parse_args()
    choose_parallelism(args)
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, time.strftime("run-%Y%m%d-%H%M%S.log"))
    start = time.time()
    conf_before = conf_stamps()
    returncode, dead_jvms = run_sbt(args, log_path)
    results = collect_reports(start)
    update_durations(results)
    print_summary(results, start, returncode, log_path, dead_jvms)
    conf_after = conf_stamps()
    touched = sorted(name for name in set(conf_before) | set(conf_after)
                     if conf_before.get(name) != conf_after.get(name))
    if touched:
        print(f"Warning: tests wrote etc/conf: {', '.join(touched)}. A test likely started a "
              "server without its own configurationFolder.")
    return returncode


if __name__ == "__main__":
    sys.exit(main())
