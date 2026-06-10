#!/usr/bin/env python3
"""
Read UC Spark version metadata for build scripts and GitHub Actions workflows.

UC keeps Spark version policy in project/spark-versions.json. This helper is the
workflow-facing API for that metadata, including the source-build cache fields
used by the Spark artifact producer and consumer jobs.
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


def version_specs_by_full_version(data):
    return {spec["version"]: spec for spec in data["versions"]}


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


def matrix_row_from_published(row):
    return {
        "spark-version": row["sparkVersion"],
        "delta-version": row["deltaVersion"],
        "validation-mode": row["validationMode"],
        "non-blocking": row["nonBlocking"],
        # Published Spark artifacts: the single test job skips the Spark build step.
        "source-build": False,
    }


def matrix_row_from_source_build(spark_version, template):
    return {
        "spark-version": spark_version,
        "delta-version": template["deltaVersion"],
        "validation-mode": template["validationMode"],
        "non-blocking": template["nonBlocking"],
        # Source-built Spark: the test job restores the cache and builds on miss.
        "source-build": True,
    }


def generate_ci_test_matrix(data):
    """Build one flat CI matrix over all Spark rows.

    Each row carries a source-build flag so a single test job can decide whether
    to restore/build the Spark Maven cache before running tests, instead of
    splitting published and source-built lanes into separate jobs.
    """
    ci_test_matrix = data.get("ciTestMatrix")
    if not ci_test_matrix:
        raise SystemExit(
            "ciTestMatrix is missing from project/spark-versions.json"
        )

    versions_by_name = version_specs_by_full_version(data)
    rows = []
    for row in ci_test_matrix.get("published", []):
        spark_version = row["sparkVersion"]
        if spark_version not in versions_by_name:
            raise SystemExit(
                "ciTestMatrix.published references unknown Spark version {}".format(
                    spark_version
                )
            )
        rows.append(matrix_row_from_published(row))

    source_build_versions = [
        spec["version"] for spec in data["versions"]
        if spec.get("sourceBuildDefaultRef")
    ]
    source_build_template = ci_test_matrix.get("sourceBuild")
    if source_build_versions:
        if not source_build_template:
            raise SystemExit(
                "ciTestMatrix.sourceBuild is required when source-build Spark versions "
                "are configured in project/spark-versions.json"
            )
        for spark_version in source_build_versions:
            rows.append(
                matrix_row_from_source_build(spark_version, source_build_template)
            )

    return rows


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
    parser = argparse.ArgumentParser(description="Read Spark version metadata")
    parser.add_argument(
        "--all-spark-versions",
        action="store_true",
        help="Output all configured Spark versions as a JSON array",
    )
    parser.add_argument(
        "--released-spark-versions",
        action="store_true",
        help="Output released Spark versions as a JSON array",
    )
    parser.add_argument(
        "--source-build-spark-versions",
        action="store_true",
        help="Output Spark versions with configured source-build default refs",
    )
    parser.add_argument(
        "--non-source-build-spark-versions",
        action="store_true",
        help="Output Spark versions without configured source-build default refs",
    )
    parser.add_argument(
        "--ci-test-matrix",
        action="store_true",
        help="Output CI test matrix rows for published and source-build lanes",
    )
    parser.add_argument(
        "--get-field",
        nargs=2,
        metavar=("SPARK_VERSION", "FIELD"),
        help="Get a specific field for a Spark version",
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

    if args.all_spark_versions:
        print(json.dumps([spec["version"] for spec in data["versions"]]))
    elif args.released_spark_versions:
        pre_release_markers = ("-SNAPSHOT", "-preview")
        print(json.dumps([
            spec["version"] for spec in data["versions"]
            if not any(marker in spec["version"] for marker in pre_release_markers)
        ]))
    elif args.source_build_spark_versions:
        print(json.dumps([
            spec["version"] for spec in data["versions"]
            if spec.get("sourceBuildDefaultRef")
        ]))
    elif args.non_source_build_spark_versions:
        print(json.dumps([
            spec["version"] for spec in data["versions"]
            if not spec.get("sourceBuildDefaultRef")
        ]))
    elif args.ci_test_matrix:
        print(json.dumps(generate_ci_test_matrix(data)))
    elif args.get_field:
        spark_version, field = args.get_field
        spec = find_spark_spec(data, spark_version)
        if field not in spec:
            print(
                "ERROR: Field '{}' not found for Spark version {}\nAvailable fields: {}".format(
                    field, spark_version, ", ".join(sorted(spec.keys()))
                ),
                file=sys.stderr,
            )
            sys.exit(1)
        print(json.dumps(spec[field]))
    elif args.resolve_source_build:
        if not args.spark_version:
            parser.error("--resolve-source-build requires --spark-version")
        resolve_source_build(args, data, repo_root)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
