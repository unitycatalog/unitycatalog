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
import signal
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
PROC_CGROUP = "/proc/self/cgroup"
# Per cgroup version: mount point, limit file, usage file, and the memory.stat key for reclaimable
# page cache, which counts as usage but is freed under pressure.
CGROUP_MEMORY = {
    "v2": ("/sys/fs/cgroup", "memory.max", "memory.current", "inactive_file"),
    "v1": ("/sys/fs/cgroup/memory", "memory.limit_in_bytes", "memory.usage_in_bytes",
           "total_inactive_file"),
}
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

    The result is also capped by what is left of this process's cgroup memory limits.
    """
    available = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    available = int(line.split()[1]) * 1024
    except OSError:
        pass
    left = cgroup_memory_left()
    return available if left is None else min(available, left)


def cgroup_memory_left():
    """Returns the memory left under this process's cgroup limits, or None if there are none.

    Checks the process's own cgroup and each parent, since every level's limit applies, and does
    not count reclaimable page cache as used.
    """
    try:
        with open(PROC_CGROUP) as f:
            entries = [line.rstrip("\n").split(":", 2) for line in f]
    except OSError:
        return None
    left = None
    for _, controllers, path in entries:
        if controllers == "":
            version = "v2"
        elif "memory" in controllers.split(","):
            version = "v1"
        else:
            continue
        mount, limit_file, usage_file, cache_key = CGROUP_MEMORY[version]
        directory = os.path.normpath(mount + path)
        while directory.startswith(mount):
            limit = read_number(os.path.join(directory, limit_file))
            # "max" (v2) or a huge value (v1) means no limit at this level.
            if limit is not None and limit < 1 << 60:
                usage = read_number(os.path.join(directory, usage_file)) or 0
                used = usage - memory_stat(directory, cache_key)
                left = limit - used if left is None else min(left, limit - used)
            if directory == mount:
                break
            directory = os.path.dirname(directory)
    return left


def read_number(path):
    try:
        with open(path) as f:
            value = f.read().strip()
    except OSError:
        return None
    return int(value) if value.isdigit() else None


def memory_stat(directory, key):
    try:
        with open(os.path.join(directory, "memory.stat")) as f:
            for line in f:
                name, _, value = line.partition(" ")
                if name == key:
                    return int(value)
    except (OSError, ValueError):
        pass
    return 0


def cpu_count():
    """Returns the CPUs this process may run on, which respects pinning such as taskset."""
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    return os.cpu_count() or 2


def memory_limit(args):
    """Returns how many test JVMs fit in the available memory, after sbt and a reserve."""
    free = available_memory() - parse_size(args.sbt_heap) * JVM_FOOTPRINT_PER_HEAP - RESERVED_BYTES
    return max(1, int(free // (parse_size(args.jvm_heap) * JVM_FOOTPRINT_PER_HEAP)))


def choose_parallelism(args):
    cpu_limit = max(1, cpu_count() // 2)
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
    # sbt gets its own process group, so the script can stop it and its test JVMs together.
    proc = subprocess.Popen(
        cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL, universal_newlines=True, errors="replace", bufsize=1,
        start_new_session=True)
    try:
        with open(log_path, "w") as log:
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
    except BaseException:
        # A failed log or terminal write, or Ctrl-C: do not leave sbt and its test JVMs running.
        stop_process_group(proc)
        raise
    finally:
        done.set()
    return returncode, dead_jvms


def stop_process_group(proc):
    """Stops proc and everything in its process group: SIGTERM, then SIGKILL after 10s."""
    for sig, timeout in ((signal.SIGTERM, 10), (signal.SIGKILL, None)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout)
            return
        except subprocess.TimeoutExpired:
            continue


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
                for row in csv.reader(f):
                    # Skip a damaged row, as the sbt side does, instead of failing every run.
                    try:
                        durations[row[0]] = float(row[1])
                    except (IndexError, ValueError):
                        continue
        durations.update({suite: seconds for suite, seconds, _, _ in results})
        os.makedirs(os.path.dirname(DURATIONS_FILE), exist_ok=True)
        # Write a temp file and rename it, so an interrupted write cannot damage the file.
        partial = DURATIONS_FILE + ".tmp"
        with open(partial, "w", newline="") as f:
            csv.writer(f).writerows(sorted(durations.items(), key=lambda kv: -kv[1]))
        os.replace(partial, DURATIONS_FILE)
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
    if returncode != 0 and not results and not dead_jvms:
        # sbt failed before any suite reported, e.g. a mistyped task: show why.
        print("\n".join(first_errors(log_path)))
    if returncode != 0:
        print(f"sbt exited with code {returncode}; see the log for details")


def first_errors(log_path, limit=10):
    """Returns the first `[error]` lines of the log, or nothing if the log cannot be read."""
    try:
        with open(log_path, errors="replace") as f:
            return [line.rstrip() for line in f if line.startswith("[error]")][:limit]
    except OSError:
        return []


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
