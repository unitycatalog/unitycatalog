#!/usr/bin/env python3
"""
Resolve source-built Spark metadata for UC CI workflows.

project/spark-versions.json owns the Spark compatibility line and the default
source ref. Workflows call this helper so producer and consumer cache keys are
derived from the same normalized values.
"""

import argparse
import json
import os
import subprocess
from pathlib import Path


def run_command(args, cwd):
    result = subprocess.run(
        args,
        cwd=str(cwd),
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    return result.stdout.strip()


def strip_suffix(value, suffix):
    if value.endswith(suffix):
        return value[: -len(suffix)]
    return value


def short_version(version):
    return ".".join(version.split(".")[:2])


def load_spark_spec(repo_root, spark_version):
    data = json.loads((repo_root / "project" / "spark-versions.json").read_text())
    versions = data["versions"]

    for spec in versions:
        if spec["version"] == spark_version:
            return spec

    short_matches = [spec for spec in versions if short_version(spec["version"]) == spark_version]
    if len(short_matches) == 1:
        return short_matches[0]

    source_build_matches = [
        spec for spec in short_matches
        if spec.get("requiresSparkCommit") or spec.get("sourceBuildDefaultRef")
    ]
    if len(source_build_matches) == 1:
        return source_build_matches[0]

    raise SystemExit(
        "Spark version {} not found unambiguously in project/spark-versions.json".format(
            spark_version
        )
    )


def resolve_spark_sha(spark_repo, spark_ref, spark_dir):
    spark_dir.mkdir(parents=True, exist_ok=True)
    if not (spark_dir / ".git").exists():
        run_command(["git", "-C", str(spark_dir), "init"], spark_dir.parent)

    try:
        run_command(["git", "-C", str(spark_dir), "remote", "get-url", "origin"], spark_dir.parent)
        run_command(
            ["git", "-C", str(spark_dir), "remote", "set-url", "origin", spark_repo],
            spark_dir.parent,
        )
    except subprocess.CalledProcessError:
        run_command(
            ["git", "-C", str(spark_dir), "remote", "add", "origin", spark_repo],
            spark_dir.parent,
        )

    run_command(["git", "-C", str(spark_dir), "fetch", "--depth", "1", "origin", spark_ref], spark_dir.parent)
    return run_command(["git", "-C", str(spark_dir), "rev-parse", "FETCH_HEAD"], spark_dir.parent)


def emit_output(values):
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as handle:
            for key, value in values.items():
                handle.write("{}={}\n".format(key, value))

    for key, value in values.items():
        print("{}={}".format(key, value))


def main():
    parser = argparse.ArgumentParser(description="Resolve source-built Spark metadata")
    parser.add_argument("--spark-version", required=True, help="Spark compatibility line")
    parser.add_argument(
        "--spark-source-ref",
        default="",
        help="Optional Spark source ref override; blank uses project/spark-versions.json",
    )
    parser.add_argument(
        "--spark-repo",
        default="https://github.com/apache/spark.git",
        help="Spark git repository",
    )
    parser.add_argument(
        "--spark-dir",
        default="/tmp/spark",
        help="Temporary Spark checkout used for ref resolution",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    spec = load_spark_spec(repo_root, args.spark_version)

    source_ref = args.spark_source_ref.strip() or spec.get("sourceBuildDefaultRef", "")
    if not source_ref:
        raise SystemExit(
            "No Spark source ref configured for Spark {}. Provide --spark-source-ref "
            "or sourceBuildDefaultRef in project/spark-versions.json.".format(args.spark_version)
        )

    artifact_base_version = spec.get("sourceBuildArtifactBaseVersion") or strip_suffix(
        spec["version"], "-SNAPSHOT"
    )
    spark_sha = resolve_spark_sha(args.spark_repo, source_ref, Path(args.spark_dir))
    spark_artifact_version = "{}-{}-SNAPSHOT".format(artifact_base_version, spark_sha[:12])

    print(
        "Resolved Spark {} source ref {} to {} and Maven version {}".format(
            spec["version"], source_ref, spark_sha, spark_artifact_version
        )
    )
    emit_output(
        {
            "spark_version": spec["version"],
            "source_ref": source_ref,
            "spark_sha": spark_sha,
            "artifact_base_version": artifact_base_version,
            "spark_artifact_version": spark_artifact_version,
        }
    )


if __name__ == "__main__":
    main()
