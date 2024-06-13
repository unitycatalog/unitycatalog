#
# DATABRICKS CONFIDENTIAL & PROPRIETARY
# __________________
#
# Copyright 2019 Databricks, Inc.
# All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
# and its suppliers, if any.  The intellectual and technical concepts contained herein are
# proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
# patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
# or reproduction of this information is strictly forbidden unless prior written permission is
# obtained from Databricks, Inc.
#
# If you view or obtain a copy of this information and believe Databricks, Inc. may not have
# intended it to be made available, please promptly report it to Databricks Legal Department
# @ legal@databricks.com.
#

import os
import subprocess
import sys
import random
import string
import tempfile

def run_copybara(
        origin_url,
        origin_ref,
        target_git_repo,
        target_branch,
        config_file,
        workflow,
        auto_commit
):
    copybara_command = download_copybara(os.path.dirname(os.path.dirname(config_file)))
        #
        # ["java", "-Xmx6g", "-jar", "/Users/tdas/Projects/copybara/bazel-bin/java/com/google/copybara/copybara_deploy.jar"]

    copybara_args = ["migrate", config_file, workflow, "--force", "--init-history",
                     "--ignore-noop"]
    locations_file = os.path.join(os.path.dirname(config_file), ".locations.bara.sky")
    locations_file_content = """
originUrl="{origin_url}"
originRef="{origin_ref}"
destinationUrl="{destination_url}"
destinationRef="{destination_ref}"
copybaraMode="{migration_mode}"
""".format(
        origin_url=origin_url,
        origin_ref=origin_ref,
        destination_url=target_git_repo,
        destination_ref=target_branch,
        migration_mode="SQUASH")

    with WorkingDirectory(os.path.dirname(config_file)):
        try:
            with open(locations_file, "w") as location_file:
                print("Writing Location File %s:\n%s" % (locations_file, locations_file_content))
                location_file.write(locations_file_content)

            if not os.path.exists(locations_file):
                raise Exception("Failed to write Locations File %s" % locations_file)

            # If we are using the current HEAD, check if there are uncommitted changes
            if origin_ref == "HEAD":
                (exit_code, _, _) = run_cmd(["git", "diff-index", "--quiet", "HEAD", "--"],
                                            throw_on_error=False)
                has_uncommitted_changes = exit_code != 0
                if has_uncommitted_changes:
                    if auto_commit:
                        print("Committing uncommitted Changes")
                        run_cmd(["git", "commit", "-am", "Copybara Auto Commit"],
                                stream_output=True)
                    else:
                        print("================ WARNING ================")
                        print("Using HEAD as the origin commit will not sync uncommitted changes")
                        print("Use --auto-commit to automatically commit changes")
                        print("=========================================")
            print("Running copybara command: %s" % str(copybara_command + copybara_args))
            run_cmd(copybara_command + copybara_args, stream_output=True)
        finally:
            if os.path.exists(locations_file):
                os.remove(locations_file)


def run_cmd(cmd, throw_on_error=True, env=None, stream_output=False, **kwargs):
    """Runs a command as a child process.

    A convenience wrapper for running a command from a Python script.
    Keyword arguments:
    cmd -- the command to run, as a list of strings
    throw_on_error -- if true, raises an Exception if the exit code of the program is nonzero
    env -- additional environment variables to be defined when running the child process
    stream_output -- if true, does not capture standard output and error; if false, captures these
      streams and returns them

    Note on the return value: If stream_output is true, then only the exit code is returned. If
    stream_output is false, then a tuple of the exit code, standard output and standard error is
    returned.
    """
    cmd_env = os.environ.copy()
    if env:
        cmd_env.update(env)

    if stream_output:
        # Flush buffered output before running the command so that the output of the command will
        # show up after the current buffered output
        sys.stdout.flush()
        sys.stderr.flush()
        child = subprocess.Popen(cmd, env=cmd_env, **kwargs)
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        return exit_code
    else:
        child = subprocess.Popen(
            cmd,
            env=cmd_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs)
        (stdout, stderr) = child.communicate()
        exit_code = child.wait()
        if throw_on_error and exit_code != 0:
            raise Exception(
                "Non-zero exitcode: %s\n\nSTDOUT:\n%s\n\nSTDERR:%s" %
                (exit_code, stdout, stderr))
        return (exit_code, stdout, stderr)


def download_copybara(download_dir):
    download_path = os.path.join(download_dir, "copybara_deploy.jar")
    if not os.path.exists(download_path):
        # Download copybara source from https://github.com/tdas/copybara/tree/updated_by_td
        # and build it.
        print("Downloading and building copybara at %s" % download_path)
        download_script = os.path.join(download_dir, "build_copybara.sh")
        run_cmd(["sh", "-c", download_script])
        if not os.path.exists(download_path):
            print("Run %s manually and ensure copybara JAR is present at %s" %
                  (download_script, download_path))

    copybara_cmd = ["java", "-Xmx10g", "-jar", download_path]
    return copybara_cmd


def create_random_branch_name():
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(8))


def initialize_empty_git_repo():
    git_repo = tempfile.mkdtemp()
    with WorkingDirectory(git_repo):
        run_cmd(["git", "init"])
        branch_name = create_random_branch_name()

    return git_repo, branch_name


# pylint: disable=too-few-public-methods
class WorkingDirectory(object):
    def __init__(self, working_directory):
        self.working_directory = working_directory
        self.old_workdir = os.getcwd()

    def __enter__(self):
        os.chdir(self.working_directory)

    def __exit__(self, tpe, value, traceback):
        os.chdir(self.old_workdir)
