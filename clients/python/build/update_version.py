#!/usr/bin/env python3

import re
import os
import sys

def read_sbt_version(version_sbt_path):
    with open(version_sbt_path, 'r') as f:
        for line in f:
            match = re.match(r'.*version.*[:=]\s*"([^"]+)"', line)
            if match:
                return match.group(1)
    raise ValueError(f"Could not find version in {version_sbt_path}")

def convert_version_for_python(sbt_version):
    if sbt_version.endswith('-SNAPSHOT'):
        return sbt_version[:-9] + '.dev0'
    else:
        return sbt_version

def update_version_in_file(file_path, python_version):
    with open(file_path, 'r') as f:
        content = f.read()

    pattern = re.compile(r'^(.*version\s*=\s*["\'])(.*?)(["\'])', flags=re.MULTILINE)

    def replace_version(match):
        return f"{match.group(1)}{python_version}{match.group(3)}"

    content_new = pattern.sub(replace_version, content)

    with open(file_path, 'w') as f:
        f.write(content_new)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    version_sbt_path = os.path.abspath(os.path.join(script_dir, '..', '..', '..', 'version.sbt'))

    if not os.path.exists(version_sbt_path):
        print(f"Error: version.sbt file not found at {version_sbt_path}")
        sys.exit(1)

    sbt_version = read_sbt_version(version_sbt_path)
    print(f"SBT version: {sbt_version}")

    python_version = convert_version_for_python(sbt_version)
    print(f"Python version: {python_version}")

    files_to_update = [
        os.path.join(script_dir, 'pyproject.toml'),
        os.path.join(script_dir, 'setup.py'),
    ]

    for file_path in files_to_update:
        if os.path.exists(file_path):
            update_version_in_file(file_path, python_version)
            print(f"Updated version in {file_path} to {python_version}")
        else:
            print(f"File not found: {file_path}")
            sys.exit(1)

if __name__ == '__main__':
    main()
