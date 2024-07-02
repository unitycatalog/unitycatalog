#!/usr/bin/env bash
set -ex;

current_dir=$(pwd)
script_dir=$(dirname $0)

docker_build=$(realpath "$script_dir/build-uc")
docker_run=$(realpath "$script_dir/run-uc")

echo "Changing directory to $docker_build"
cd $docker_build

if [[ -r Dockerfile ]]; then
    docker buildx build -t unitycatalog_jar:0.1.0 .
else
    echo "Dockerfile not found."
    exit
fi

echo "Changing directory to $docker_run"
cd $docker_run

if [[ -r Dockerfile ]]; then
    docker buildx build -t unitycatalog:0.1.0 .
else
    echo "Dockerfile not found."
    exit
fi


echo "Returning to base directory $current_dir"
cd $current_dir