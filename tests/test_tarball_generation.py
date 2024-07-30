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
import threading


def stopServer():
    try:
        with open("server_pid.txt", "r") as f:
            pid = int(f.read().strip())
            os.killpg(os.getpgid(pid), signal.SIGTERM)  # Send SIGTERM to the process group
            print(f"Stopped server with PID {pid}.")
    except Exception as e:
        print(f"Failed to stop the server: {e}")

# 1. Extract the tarball
tarball_path = "target/unitycatalog-{version}.tar.gz"
extract_path = "target/dist"

try:
    version_pattern = r'version\s*:=\s*"([^"]+)"'
    with open('version.sbt', 'r') as file:
        content = file.read()
        match = re.search(version_pattern, content) # Extract the version string
        if match:
            version = match.group(1)
            tarball_path = tarball_path.format(version=version)
        else:
            print('Version string not found.')
            sys.exit(1)
except Exception as e:
    print(f"Failed to extract version string: {e}")
    sys.exit(1)

if not os.path.exists(extract_path):
    os.makedirs(extract_path)

print(f"Load tarball from {tarball_path}")
with tarfile.open(tarball_path, "r:gz") as tar:
    tar.extractall(path=extract_path, filter='data')
    print("Extracted tarball to {}".format(extract_path))

# 2. Check executable permissions
bin_dir = extract_path + "/bin"
for filename in os.listdir(bin_dir):
    filepath = os.path.join(bin_dir, filename)
    if os.path.isfile(filepath):
        os.chmod(filepath, 0o755)
        print(f"Set executable permission for {filepath}")

# 3. Start server script
def read_output(pipe, output):
    for line in iter(pipe.readline, b''):
        output.append(line.decode())
    pipe.close()

start_server_cmd = os.path.join(bin_dir, "start-uc-server")
server_process = subprocess.Popen(
    [start_server_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid
)

with open("server_pid.txt", "w") as f:
    f.write(str(server_process.pid))
print(f"Server started with PID {server_process.pid}")

stdout_lines = []
stderr_lines = []
stdout_thread = threading.Thread(target=read_output, args=(server_process.stdout, stdout_lines))
stderr_thread = threading.Thread(target=read_output, args=(server_process.stderr, stderr_lines))
stdout_thread.start()
stderr_thread.start()
time.sleep(15)
stdout_thread.join(timeout=5)
stderr_thread.join(timeout=5)

if not stderr_lines:
    print("Server started successfully.")
    print("".join(stdout_lines))
else:
    print("Server failed to start.")
    print("".join(stderr_lines))
    stopServer()
    exit(1)

# 4. Verify server is running
try:
    response = requests.head("http://localhost:8081", timeout=5)
    if response.status_code == 200:
        print("Server is running.")
    else:
        print(f"Server responded with status code: {response.status_code}")
        stopServer()
        sys.exit(1)
except requests.RequestException as e:
    print(f"Failed to connect to the server: {e}")
    stopServer()
    sys.exit(1)

# 5. Run and verify CLI
try:
    cli_cmd = [os.path.join(bin_dir, "uc"), "catalog", "create", "--name", "Test_Catalog"]
    subprocess.run(cli_cmd, check=True, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print("CLI command executed successfully.")
except subprocess.CalledProcessError as e:
    print(f"CLI command failed with error: {e}")
    stopServer()
    sys.exit(1)

# 6. Stop server
stopServer()

# 7. Cleanup temp directory
try:
    shutil.rmtree(extract_path)
    print(f"Deleted target directory at {extract_path}.")
except Exception as e:
    print(f"Failed to delete target directory: {e}")
