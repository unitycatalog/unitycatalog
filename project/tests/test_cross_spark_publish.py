#!/usr/bin/env python3
"""
Cross-Spark Version Build Testing for Unity Catalog

Tests the UC build system by validating JAR file names for:
1. Default publish (publishM2) - publishes Spark module WITH Spark suffix
2. Backward-compat publish (skipSparkSuffix=true) - publishes WITHOUT suffix
3. Per-version publish validates the correct suffix for each Spark version

Usage:
    python project/tests/test_cross_spark_publish.py

The script will:
1. Load Spark versions from project/spark-versions.json (shared source of truth)
2. Test default publishM2 publishes Spark module WITH suffix
3. Test skipSparkSuffix=true publishes WITHOUT suffix (backward compatibility)
4. Test per-version publish for each non-snapshot Spark version
5. Exit with status 0 on success, 1 on failure
"""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Set

# Spark-related module (gets a Spark version suffix)
# Template format: {suffix} = short Spark version suffix (e.g., "", "_4.0")
#                  {version} = full UC version (e.g., "0.5.0-SNAPSHOT")
SPARK_RELATED_JAR_TEMPLATES = [
    "unitycatalog-spark{suffix}_2.13-{version}.jar",
]

# Non-Spark-related modules (built once, same for all Spark versions)
NON_SPARK_RELATED_JAR_TEMPLATES = [
    "unitycatalog-client-{version}.jar",
    "unitycatalog-server-{version}.jar",
    "unitycatalog-hadoop-{version}.jar",
]


class SparkVersionSpec(object):
    """Configuration for a specific Spark version."""

    def __init__(self, suffix, requires_spark_commit=False):
        self.suffix = suffix
        self.requires_spark_commit = requires_spark_commit
        self.spark_related_jars = [
            jar.format(suffix=self.suffix, version="{version}")
            for jar in SPARK_RELATED_JAR_TEMPLATES
        ]
        self.non_spark_related_jars = list(NON_SPARK_RELATED_JAR_TEMPLATES)

    @property
    def all_jars(self):
        return self.spark_related_jars + self.non_spark_related_jars


def _load_spark_versions():
    """Loads Spark version specs from the shared JSON source of truth."""
    json_path = Path(__file__).parent.parent / "spark-versions.json"
    with open(json_path) as f:
        data = json.load(f)
    versions = {}
    for entry in data["versions"]:
        ver = entry["version"]
        short = "_" + ".".join(ver.split(".")[:2])
        versions[ver] = SparkVersionSpec(
            suffix=short,
            requires_spark_commit=entry.get("requiresSparkCommit", False),
        )
    return data["default"], versions


DEFAULT_SPARK, SPARK_VERSIONS = _load_spark_versions()


def substitute_version(jar_templates, uc_version):
    return {jar.format(version=uc_version) for jar in jar_templates}


class CrossSparkPublishTest:
    """Tests cross-Spark version builds for Unity Catalog."""

    def __init__(self, uc_root: Path):
        self.uc_root = uc_root
        self.uc_version = self._get_uc_version()

    def _get_uc_version(self) -> str:
        version_re = re.compile(r'version\s*:=\s*"([^"]+)"')
        with open(self.uc_root / "version.sbt", "r") as f:
            for line in f:
                m = version_re.search(line)
                if m:
                    return m.group(1)
        sys.exit("Error: Could not parse version from version.sbt")

    def clean_maven_cache(self) -> None:
        m2_repo = Path.home() / ".m2" / "repository" / "io" / "unitycatalog"
        if m2_repo.exists():
            print(f"Cleaning Maven cache: {m2_repo}")
            shutil.rmtree(m2_repo)
            print("  Maven cache cleaned\n")
        else:
            print("  Maven cache already clean\n")

    def find_spark_jars(self) -> Set[str]:
        """Finds UC Spark connector JAR files from Maven local repository."""
        m2_repo = Path.home() / ".m2" / "repository" / "io" / "unitycatalog"
        if not m2_repo.exists():
            return set()

        found_jars = set()
        for version_dir in m2_repo.rglob(self.uc_version):
            for jar_file in version_dir.glob("*.jar"):
                if not any(x in jar_file.name for x in ["-tests", "-sources", "-javadoc"]):
                    # Only include spark connector JARs
                    if "unitycatalog-spark" in jar_file.name:
                        found_jars.add(jar_file.name)
        return found_jars

    def run_sbt_command(self, description: str, command: List[str]) -> bool:
        # The Test-scoped delta-spark dependency is only required for running tests,
        # not for publishing. Some Spark versions (e.g. previews) have no matching
        # Delta release yet, which would otherwise fail `update` during `publishM2`.
        # `publishM2` doesn't include Test deps in the POM, so skipping is safe here.
        augmented = list(command)
        if augmented and augmented[0].endswith("sbt"):
            augmented.insert(1, "-DskipDeltaSpark=true")
        print(f"  {description}")
        try:
            subprocess.run(
                augmented,
                cwd=self.uc_root,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT,
            )
            return True
        except subprocess.CalledProcessError:
            print(f"  FAIL: Command failed: {' '.join(augmented)}")
            return False

    def validate_jars(self, expected: Set[str], test_name: str) -> bool:
        found = self.find_spark_jars()

        print(f"\n{test_name} - Found JARs ({len(found)} total):")
        for jar in sorted(found):
            print(f"  {jar}")

        print(f"\n{test_name} - Expected JARs ({len(expected)} total):")
        for jar in sorted(expected):
            print(f"  {jar}")

        missing = expected - found
        extra = found - expected

        print()
        if not missing and not extra:
            print(f"PASS: {test_name} - All expected JARs found")
            return True

        if missing:
            print(f"FAIL: {test_name} - Missing JARs ({len(missing)}):")
            for jar in sorted(missing):
                print(f"  MISSING: {jar}")

        if extra:
            print(f"\nFAIL: {test_name} - Unexpected JARs ({len(extra)}):")
            for jar in sorted(extra):
                print(f"  EXTRA: {jar}")

        return False

    def test_default_publish(self) -> bool:
        """Default publishM2 should publish spark module WITH Spark suffix."""
        spark_spec = SPARK_VERSIONS[DEFAULT_SPARK]

        print("\n" + "=" * 70)
        print(
            f"TEST: Default spark/publishM2 (should publish WITH suffix for Spark {DEFAULT_SPARK})"
        )
        print("=" * 70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            "Running: build/sbt spark/publishM2",
            ["build/sbt", "spark/publishM2"],
        ):
            return False

        expected = substitute_version(spark_spec.spark_related_jars, self.uc_version)
        return self.validate_jars(expected, "Default spark/publishM2 (with suffix)")

    def test_backward_compat_publish(self) -> bool:
        """skipSparkSuffix=true should publish spark module WITHOUT suffix."""
        spark_spec_no_suffix = SparkVersionSpec(suffix="")

        print("\n" + "=" * 70)
        print("TEST: skipSparkSuffix=true (backward compatibility - no suffix)")
        print("=" * 70)

        self.clean_maven_cache()

        if not self.run_sbt_command(
            "Running: build/sbt -DskipSparkSuffix=true spark/publishM2",
            ["build/sbt", "-DskipSparkSuffix=true", "spark/publishM2"],
        ):
            return False

        expected = substitute_version(
            spark_spec_no_suffix.spark_related_jars, self.uc_version
        )
        return self.validate_jars(
            expected, "skipSparkSuffix=true (backward compat)"
        )

    def test_per_version_publish(self) -> bool:
        """Each non-snapshot Spark version should produce correctly-suffixed JARs."""
        print("\n" + "=" * 70)
        print("TEST: Per-version publish (each non-snapshot Spark version)")
        print("=" * 70)

        all_passed = True
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            if "SNAPSHOT" in spark_version or spark_spec.requires_spark_commit:
                print(f"\n  Skipping source-built or snapshot version: {spark_version}")
                continue

            self.clean_maven_cache()

            if not self.run_sbt_command(
                f"Running: build/sbt -DsparkVersion={spark_version} spark/publishM2",
                [
                    "build/sbt",
                    f"-DsparkVersion={spark_version}",
                    "spark/publishM2",
                ],
            ):
                all_passed = False
                continue

            expected = substitute_version(
                spark_spec.spark_related_jars, self.uc_version
            )
            if not self.validate_jars(
                expected, f"Spark {spark_version} (suffix={spark_spec.suffix})"
            ):
                all_passed = False

        return all_passed

    def test_cross_spark_workflow(self) -> bool:
        """Full cross-Spark workflow: backward-compat + all non-snapshot with suffix."""
        print("\n" + "=" * 70)
        print("TEST: Cross-Spark Workflow (backward-compat + all with suffix)")
        print("=" * 70)

        self.clean_maven_cache()

        # Step 1: Publish WITHOUT suffix (backward compatibility)
        if not self.run_sbt_command(
            "Step 1: build/sbt -DskipSparkSuffix=true spark/publishM2 (no suffix)",
            ["build/sbt", "-DskipSparkSuffix=true", "spark/publishM2"],
        ):
            return False

        # Step 2: Publish WITH suffix for each non-snapshot version
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            if "SNAPSHOT" in spark_version or spark_spec.requires_spark_commit:
                continue
            if not self.run_sbt_command(
                f"Step 2: build/sbt -DsparkVersion={spark_version} spark/publishM2",
                [
                    "build/sbt",
                    f"-DsparkVersion={spark_version}",
                    "spark/publishM2",
                ],
            ):
                return False

        # Build expected JARs
        expected: Set[str] = set()

        # Step 1: Without suffix
        no_suffix_spec = SparkVersionSpec(suffix="")
        expected.update(
            substitute_version(no_suffix_spec.spark_related_jars, self.uc_version)
        )

        # Step 2: With suffix for each non-snapshot
        for spark_version, spark_spec in SPARK_VERSIONS.items():
            if "SNAPSHOT" in spark_version or spark_spec.requires_spark_commit:
                continue
            expected.update(
                substitute_version(spark_spec.spark_related_jars, self.uc_version)
            )

        return self.validate_jars(expected, "Cross-Spark Workflow")


def main():
    try:
        uc_root = Path(__file__).parent.parent.parent
        if not (uc_root / "build.sbt").exists():
            print("Error: build.sbt not found. Run from UC repository root.")
            sys.exit(1)

        print("=" * 70)
        print("Unity Catalog Cross-Spark Build Test Suite")
        print("=" * 70)
        print()

        test = CrossSparkPublishTest(uc_root)

        t1 = test.test_default_publish()
        t2 = test.test_backward_compat_publish()
        t3 = test.test_per_version_publish()
        t4 = test.test_cross_spark_workflow()

        print("\n" + "=" * 70)
        print("TEST SUMMARY")
        print("=" * 70)
        print(
            f"  Default publishM2 (with suffix):        {'PASS' if t1 else 'FAIL'}"
        )
        print(
            f"  skipSparkSuffix (backward compat):      {'PASS' if t2 else 'FAIL'}"
        )
        print(
            f"  Per-version publish:                    {'PASS' if t3 else 'FAIL'}"
        )
        print(
            f"  Cross-Spark Workflow (both):            {'PASS' if t4 else 'FAIL'}"
        )
        print("=" * 70)

        if t1 and t2 and t3 and t4:
            print("\nALL TESTS PASSED")
            sys.exit(0)
        else:
            print("\nSOME TESTS FAILED")
            sys.exit(1)

    except Exception as e:
        print(f"\nTEST EXECUTION FAILED WITH ERROR: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
