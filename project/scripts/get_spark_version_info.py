#!/usr/bin/env python3
"""
Resolve source-build Spark cache metadata for UC's CI workflow.

UC keeps Spark version policy in project/spark-versions.json. This helper reads the
source-build fields for a Spark version and emits the immutable Spark SHA, the
commit-qualified local Maven version, and the exact Maven cache key used by the
prepare-spark-env action.
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
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


def load_spark_versions(repo_root):
    with open(repo_root / "project" / "spark-versions.json", "r") as handle:
        return json.load(handle)


def find_spark_spec(data, spark_version):
    versions = data["versions"]

    if spark_version == "default":
        spark_version = data["default"]

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


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compute_spark_m2_cache_key(
    runner_label,
    spark_version,
    artifact_base_version,
    spark_sha,
    build_script_path,
):
    script_hash = sha256_file(build_script_path)
    return (
        "spark-m2-{}-scala-2.13-"
        "{}-{}-{}-{}".format(
            runner_label,
            spark_version,
            artifact_base_version,
            spark_sha,
            script_hash,
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


def resolve_source_build(args, data, repo_root):
    spec = find_spark_spec(data, args.spark_version)
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
    build_script_path = repo_root / "project" / "scripts" / "build_spark.sh"
    cache_key = compute_spark_m2_cache_key(
        args.runner_label,
        spec["version"],
        artifact_base_version,
        spark_sha,
        build_script_path,
    )

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
            "cache_key": cache_key,
        }
    )


def main():
    parser = argparse.ArgumentParser(
        description="Resolve source-build Spark cache metadata for CI"
    )
    parser.add_argument(
        "--resolve-source-build",
        action="store_true",
        help="Resolve source-build cache metadata and emit GitHub outputs",
    )
    parser.add_argument("--spark-version", help="Spark compatibility line for --resolve-source-build")
    parser.add_argument(
        "--spark-source-ref",
        default="",
        help="Optional Spark source ref override for --resolve-source-build",
    )
    parser.add_argument(
        "--spark-repo",
        default="https://github.com/apache/spark.git",
        help="Spark git repository for --resolve-source-build",
    )
    parser.add_argument(
        "--spark-dir",
        default="/tmp/spark",
        help="Temporary Spark checkout used for source-ref resolution",
    )
    parser.add_argument(
        "--runner-label",
        default="ubuntu-latest",
        help="GitHub Actions runner label used in Spark Maven cache keys",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    data = load_spark_versions(repo_root)

    if args.resolve_source_build:
        if not args.spark_version:
            parser.error("--resolve-source-build requires --spark-version")
        resolve_source_build(args, data, repo_root)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
