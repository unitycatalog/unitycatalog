#!/usr/bin/env python3

import os
import re
import requests
import shutil
import signal
import subprocess
import sys
import tarfile
import time

# Public server port (the URL transcoder; it forwards to the internal Armeria
# port at PORT + 1). bin/start-uc-server launches via UnityCatalogServer.main,
# which serves the public API on this port.
SERVER_PORT = 8080
READINESS_PATH = "/api/2.1/unity-catalog/catalogs"
READINESS_TIMEOUT_SECONDS = 60


def stop_server(pid):
    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)  # SIGTERM to the process group
        print(f"Stopped server with PID {pid}.")
    except Exception as e:
        print(f"Failed to stop the server: {e}")


# 1. Resolve the tarball path from version.sbt
tarball_path = "target/unitycatalog-{version}.tar.gz"
extract_path = "target/dist"

try:
    version_pattern = r'version\s*:=\s*"([^"]+)"'
    with open("version.sbt", "r") as file:
        match = re.search(version_pattern, file.read())
        if not match:
            print("Version string not found.")
            sys.exit(1)
        tarball_path = tarball_path.format(version=match.group(1))
except Exception as e:
    print(f"Failed to extract version string: {e}")
    sys.exit(1)

# 2. Extract the tarball
if os.path.exists(extract_path):
    shutil.rmtree(extract_path)
os.makedirs(extract_path)

print(f"Loading tarball from {tarball_path}")
with tarfile.open(tarball_path, "r:gz") as tar:
    tar.extractall(path=extract_path, filter="data")
    print(f"Extracted tarball to {extract_path}")

# 3. Ensure the launcher scripts are executable
bin_dir = os.path.join(extract_path, "bin")
for filename in os.listdir(bin_dir):
    filepath = os.path.join(bin_dir, filename)
    if os.path.isfile(filepath):
        os.chmod(filepath, 0o755)

# 4. Start the server (in its own process group so we can stop the whole tree)
start_server_cmd = os.path.join(bin_dir, "start-uc-server")
server_process = subprocess.Popen([start_server_cmd], preexec_fn=os.setsid)
pid = server_process.pid
print(f"Server started with PID {pid}")

# 5. Wait until the server is serving. Any HTTP response (even 401/404) means it
#    is up; a connection error means not-yet-ready. Fail fast if it exits early.
base_url = f"http://localhost:{SERVER_PORT}{READINESS_PATH}"
deadline = time.time() + READINESS_TIMEOUT_SECONDS
ready = False
while time.time() < deadline:
    if server_process.poll() is not None:
        print(f"Server exited early with code {server_process.returncode}.")
        sys.exit(1)
    try:
        response = requests.get(base_url, timeout=5)
        print(f"Server is serving on {SERVER_PORT} (HTTP {response.status_code}).")
        ready = True
        break
    except requests.RequestException:
        time.sleep(2)

if not ready:
    print(f"Server did not become ready within {READINESS_TIMEOUT_SECONDS}s.")
    stop_server(pid)
    sys.exit(1)

# 6. Run and verify the CLI against the running server
try:
    cli_cmd = [
        os.path.join(bin_dir, "uc"),
        "catalog",
        "create",
        "--name",
        "Test_Catalog",
    ]
    subprocess.run(cli_cmd, check=True)
    print("CLI command executed successfully.")
except subprocess.CalledProcessError as e:
    print(f"CLI command failed with error: {e}")
    stop_server(pid)
    sys.exit(1)

# 7. Stop the server and clean up
stop_server(pid)
try:
    shutil.rmtree(extract_path)
    print(f"Deleted temp directory {extract_path}.")
except Exception as e:
    print(f"Failed to delete temp directory: {e}")
