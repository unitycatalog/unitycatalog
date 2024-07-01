import os
import subprocess
import time
import requests
import signal
import tarfile
import shutil

# 1. Extract the tarball
tarball_path = "target/unitycatalog-0.1.0.tar.gz"
extract_path = "target/dist"

if not os.path.exists(extract_path):
    os.makedirs(extract_path)

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
start_server_cmd = os.path.join(bin_dir, "start-uc-server")
server_process = subprocess.Popen([start_server_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
with open("server_pid.txt", "w") as f:
    f.write(str(server_process.pid))
print(f"Server started with PID {server_process.pid}")
time.sleep(10)  # Give the server some time to start

# 4. Verify server is running
try:
    response = requests.head("http://localhost:8081", timeout=60)
    if response.status_code == 200:
        print("Server is running.")
    else:
        print(f"Server responded with status code: {response.status_code}")
except requests.RequestException as e:
    print(f"Failed to connect to the server: {e}")

# 5. Run and verify CLI
try:
    cli_cmd = [os.path.join(bin_dir, "uc"), "catalog", "create", "--name", "Test Catalog"]
    subprocess.run(cli_cmd, check=True, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print("CLI command executed successfully.")
except subprocess.CalledProcessError as e:
    print(f"CLI command failed with error: {e}")

# 6. Stop server
try:
    with open("server_pid.txt", "r") as f:
        pid = int(f.read().strip())
        os.killpg(os.getpgid(pid), signal.SIGTERM)  # Send SIGTERM to the process group
        print(f"Stopped server with PID {pid}.")
except Exception as e:
    print(f"Failed to stop the server: {e}")

# 7. Cleanup temp directory
try:
    shutil.rmtree(extract_path)
    print(f"Deleted target directory at {extract_path}.")
except Exception as e:
    print(f"Failed to delete target directory: {e}")