#!/usr/bin/env python3
"""
Pre-release testing script for Unity Catalog.

Environment variables:
- UC_SERVER_HOST: UC server host (default: localhost)
- UC_SERVER_PORT: UC server port (default: 8080)
- UC_TOKEN: Authentication token (default: empty)
- SPARK_HOME: Path to Spark installation (required for Spark SQL tests)
- UC_VERSION: UC Spark package version (default: 0.3.1-SNAPSHOT)
- DELTA_VERSION: Delta Spark version (default: 4.0.1-SNAPSHOT)
- SCALA_VERSION: Scala version (default: 2.13)
"""

import os
import sys
import subprocess
import time
import signal
import urllib.request
import urllib.error

# Configuration
UC_HOST = os.getenv("UC_SERVER_HOST", "localhost")
UC_PORT = os.getenv("UC_SERVER_PORT", "8080")
UC_URL = f"http://{UC_HOST}:{UC_PORT}"
UC_TOKEN = os.getenv("UC_TOKEN", "")
SPARK_HOME = os.getenv("SPARK_HOME", "")
UC_VERSION = os.getenv("UC_VERSION", "0.3.1-SNAPSHOT")
DELTA_VERSION = os.getenv("DELTA_VERSION", "4.0.1-SNAPSHOT")
SCALA_VERSION = os.getenv("SCALA_VERSION", "2.13")

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def log(msg, level="INFO"):
    print(f"[{level}] {msg}")


def run_cmd(cmd, timeout=30):
    """Run command, return (success, stdout, stderr)."""
    try:
        r = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)
        return r.returncode == 0, r.stdout, r.stderr
    except subprocess.TimeoutExpired:
        return False, "", "timeout"
    except Exception as e:
        return False, "", str(e)


def kill_port(port):
    """Kill all processes on the given port."""
    try:
        r = subprocess.run(["lsof", "-ti", f":{port}"], capture_output=True, text=True, timeout=5)
        for pid in r.stdout.strip().split('\n'):
            if pid:
                log(f"Killing PID {pid} on port {port}")
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except (ProcessLookupError, ValueError):
                    pass
    except Exception:
        pass


def wait_for_server(max_attempts=30):
    """Wait for UC server to be ready."""
    log(f"Waiting for server at {UC_URL}")
    for i in range(1, max_attempts + 1):
        try:
            resp = urllib.request.urlopen(f"{UC_URL}/api/2.1/unity-catalog/catalogs", timeout=5)
            if resp.getcode() == 200:
                log(f"Server ready (attempt {i})")
                return True
        except urllib.error.HTTPError as e:
            if e.code in [401, 403]:
                log(f"Server ready (attempt {i})")
                return True
        except Exception:
            pass
        time.sleep(2)
    log("Server failed to start", "ERROR")
    return False


def run_spark_sql(query, timeout=300):
    """Run a Spark SQL query and return (success, stdout, stderr)."""
    if not SPARK_HOME:
        return False, "", "SPARK_HOME not set"
    spark_sql = os.path.join(SPARK_HOME, "bin", "spark-sql")
    if not os.path.exists(spark_sql):
        return False, "", f"spark-sql not found: {spark_sql}"
    cmd = [
        spark_sql, "--name", "local-uc-test", "--master", "local[*]",
        "--packages", f"io.delta:delta-spark_{SCALA_VERSION}:{DELTA_VERSION},io.unitycatalog:unitycatalog-spark_{SCALA_VERSION}:{UC_VERSION}",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=io.unitycatalog.spark.UCSingleCatalog",
        "--conf", "spark.sql.catalog.unity=io.unitycatalog.spark.UCSingleCatalog",
        "--conf", f"spark.sql.catalog.unity.uri={UC_URL}",
        "--conf", f"spark.sql.catalog.unity.token={UC_TOKEN}",
        "--conf", "spark.sql.defaultCatalog=unity",
        "-e", query
    ]
    return run_cmd(cmd, timeout=timeout)


def cleanup():
    log("Stopping UC server...")
    kill_port(UC_PORT)


def test_start_server():
    """Step 1: Start UC server."""
    print("\n" + "=" * 50 + "\nStep 1: Start UC Server\n" + "=" * 50)
    script = os.path.join(REPO_ROOT, "bin", "start-uc-server")
    if not os.path.exists(script):
        log(f"Not found: {script}", "ERROR")
        return False
    log(f"Starting: {script}")
    subprocess.Popen([script], cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return wait_for_server()


def test_uc_cli():
    """Step 2: Verify UC CLI."""
    print("\n" + "=" * 50 + "\nStep 2: Verify UC CLI\n" + "=" * 50)
    cli = os.path.join(REPO_ROOT, "bin", "uc")
    if not os.path.exists(cli):
        log(f"Not found: {cli}", "ERROR")
        return False
    cmd = [cli, "table", "list", "--catalog", "unity", "--schema", "default"]
    log(f"Running: {' '.join(cmd)}")
    ok, out, err = run_cmd(cmd)
    if out:
        print(out)
    if not ok:
        log(f"Failed: {err}", "ERROR")
    return ok


def test_spark_sql_query():
    """Step 3 & 4: Run Spark SQL query on marksheet table."""
    print("\n" + "=" * 50 + "\nStep 3 & 4: Spark SQL Query\n" + "=" * 50)
    if not SPARK_HOME:
        log("SPARK_HOME not set, skipping", "WARN")
        return True
    query = "SELECT * FROM default.marksheet LIMIT 5;"
    log(f"Query: {query}")
    ok, out, err = run_spark_sql(query)
    if out:
        print(out)
    if not ok:
        log(f"Failed: {err}", "ERROR")
    return ok


def test_table_operations():
    """Step 5: Table operations (CREATE, INSERT, UPDATE, DELETE, DROP)."""
    print("\n" + "=" * 50 + "\nStep 5: Table Operations\n" + "=" * 50)
    if not SPARK_HOME:
        log("SPARK_HOME not set, skipping", "WARN")
        return True

    # Cleanup: delete demo schema if exists from previous run
    cli = os.path.join(REPO_ROOT, "bin", "uc")
    run_cmd([cli, "table", "delete", "--full_name", "unity.demo.mytable"], timeout=10)
    run_cmd([cli, "schema", "delete", "--full_name", "unity.demo"], timeout=10)

    # Run all queries in a single Spark session
    queries = """
CREATE SCHEMA unity.demo;
CREATE TABLE unity.demo.mytable (id INT, desc STRING) USING delta;
INSERT INTO unity.demo.mytable VALUES (1, 'test 1'), (2, 'test 2'), (3, 'test 3'), (4, 'test 4');
SELECT * FROM unity.demo.mytable;
UPDATE unity.demo.mytable SET id = 5 WHERE id = 4;
DELETE FROM unity.demo.mytable WHERE id = 5;
DROP TABLE unity.demo.mytable;
DROP SCHEMA unity.demo;
SHOW TABLES IN unity.default;
"""
    log("Running table operations...")
    ok, out, err = run_spark_sql(queries)
    if out:
        print(out)
    if not ok:
        log(f"Failed: {err}", "ERROR")
        return False

    return True


def main():
    signal.signal(signal.SIGINT, lambda *_: (cleanup(), sys.exit(1)))
    signal.signal(signal.SIGTERM, lambda *_: (cleanup(), sys.exit(1)))

    print("=" * 50 + "\nUnity Catalog Pre-Release Test\n" + "=" * 50)
    log(f"Server: {UC_URL}, UC: {UC_VERSION}, Delta: {DELTA_VERSION}, Spark: {SPARK_HOME or 'NOT SET'}")

    tests = [
        ("Start Server", test_start_server),
        ("UC CLI", test_uc_cli),
        ("Spark SQL Query", test_spark_sql_query),
        ("Table Operations", test_table_operations),
    ]
    results = []

    try:
        for name, fn in tests:
            ok = fn()
            results.append((name, ok))
            if name == "Start Server" and not ok:
                break
    finally:
        cleanup()

    print("\n" + "=" * 50 + "\nSummary\n" + "=" * 50)
    for name, ok in results:
        log(f"{'PASS' if ok else 'FAIL'}: {name}")

    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())
