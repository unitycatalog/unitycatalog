#!/usr/bin/env bash
set -ex;

current_dir=$(pwd)
script_dir=$(dirname $0)
docker_dir="$script_dir/run-uc"

echo "Changing directory to $docker_dir"
cd $docker_dir

if [[ -r Dockerfile ]]; then
    docker build -t unitycatalogue:0.1.0 .
else
    echo "Dockerfile not found."
    exit
fi

echo "Returning to base directory $current_dir"
cd $current_dir