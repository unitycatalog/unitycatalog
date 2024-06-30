#!/usr/bin/env bash
set -ex;

current_dir=$(pwd)
script_dir=$(dirname $0)

echo "Changing directory to $script_dir"
cd $script_dir

if [[ -r Dockerfile ]]; then
    docker build -t unitycatalogue:0.1.0 .
else
    echo "Dockerfile not found."
    exit 1
fi

echo "Returning to base directory $current_dir"
cd $current_dir