#!/usr/bin/env python3
"""
Unity Catalog Integration Test Runner

Automates running integration tests for Unity Catalog across AWS, Azure, and GCP.

Usage:
    ./run_integration_tests.py [--aws] [--azure] [--gcp] [--all] [--build] [--skip-validation]

Examples:
    ./run_integration_tests.py --aws              # Run AWS tests only
    ./run_integration_tests.py --aws --azure      # Run AWS and Azure tests
    ./run_integration_tests.py --all              # Run all cloud vendor tests
    ./run_integration_tests.py --all --build      # Build first, then run all tests
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Set

VENDOR_CONFIG = {
    'aws': {
        'vars': ['CATALOG_NAME', 'SCHEMA_NAME', 'CATALOG_URI', 'CATALOG_AUTH_TOKEN',
                 'CATALOG_OAUTH_URI', 'CATALOG_OAUTH_CLIENT_ID', 'CATALOG_OAUTH_CLIENT_SECRET'],
        'storage': 'S3_BASE_LOCATION',
    },
    'azure': {
        'vars': ['CATALOG_NAME', 'SCHEMA_NAME', 'CATALOG_URI', 'CATALOG_AUTH_TOKEN',
                 'CATALOG_OAUTH_URI', 'CATALOG_OAUTH_CLIENT_ID', 'CATALOG_OAUTH_CLIENT_SECRET'],
        'storage': 'ABFSS_BASE_LOCATION',
    },
    'gcp': {
        'vars': ['CATALOG_NAME', 'SCHEMA_NAME', 'CATALOG_URI', 'CATALOG_AUTH_TOKEN',
                 'CATALOG_OAUTH_URI', 'CATALOG_OAUTH_CLIENT_ID', 'CATALOG_OAUTH_CLIENT_SECRET'],
        'storage': 'GS_BASE_LOCATION',
    },
}


def header(msg: str):
    print(f"\n{'=' * 80}\n{msg}\n{'=' * 80}\n")


def validate_env(vendors: Set[str], skip: bool = False) -> bool:
    """Validate required environment variables for selected vendors."""
    header("Validating Environment Variables")

    missing = []
    for vendor in sorted(vendors):
        prefix = vendor.upper()
        print(f"\n{vendor.upper()} Environment:")

        for var in VENDOR_CONFIG[vendor]['vars']:
            full_var = f"{prefix}_{var}"
            status = "[OK]" if os.getenv(full_var) else "[MISSING]"
            print(f"  {status} {full_var}")
            if status == "[MISSING]":
                missing.append(full_var)

        storage_var = VENDOR_CONFIG[vendor]['storage']
        status = "[OK]" if os.getenv(storage_var) else "[MISSING]"
        print(f"  {status} {storage_var}")
        if status == "[MISSING]":
            missing.append(storage_var)

    if missing:
        msg = f"\nValidation skipped. Missing: {', '.join(missing)}\n" if skip else \
              f"\nERROR: Missing {len(missing)} required variable(s). Use --skip-validation to proceed anyway.\n"
        print(msg)
        return skip

    print("\nAll required environment variables are set!\n")
    return True


def build_project() -> bool:
    """Build the Unity Catalog project."""
    header("Building Unity Catalog Project")
    print("Running: ./build/sbt clean package\n")

    result = subprocess.run(['./build/sbt', '-J-Xmx8G', '-J-XX:+UseG1GC', 'clean', 'package'])
    success = result.returncode == 0

    print(f"\n{'Build completed successfully!' if success else 'ERROR: Build failed!'}\n")
    return success


def run_tests(vendor: str, log_dir: Path) -> tuple[bool, str]:
    """Run integration tests for a specific cloud vendor."""
    header(f"Running {vendor.upper()} Integration Tests")

    # Prepare environment
    env = os.environ.copy()
    prefix = vendor.upper()
    storage_var = VENDOR_CONFIG[vendor]['storage']

    for var in VENDOR_CONFIG[vendor]['vars']:
        env[var] = os.getenv(f"{prefix}_{var}", '')
    env[storage_var] = os.getenv(storage_var, '')

    # Create log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{vendor}_{timestamp}.log"

    # Print config
    uri = env.get('CATALOG_URI', 'NOT SET')
    uri_display = f"{uri[:60]}..." if len(uri) > 60 else uri

    config = f"""Test Configuration:
  CATALOG: {env.get('CATALOG_NAME', 'NOT SET')}.{env.get('SCHEMA_NAME', 'NOT SET')}
  URI: {uri_display}
  STORAGE: {env.get(storage_var, 'NOT SET')}
  Log: {log_file}
"""
    print(config)

    # Write log header
    with open(log_file, 'w') as f:
        f.write(f"Unity Catalog Integration Test - {vendor.upper()}\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'=' * 80}\n\n{config}\n{'=' * 80}\n\n")

    # Run tests with real-time output
    with open(log_file, 'a') as f:
        process = subprocess.Popen(
            ['./build/sbt', '-J-Xmx8G', '-J-XX:+UseG1GC', 'integrationTests/test'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            print(line, end='')
            f.write(line)

        return_code = process.wait()
        success = return_code == 0

        if not success:
            f.write(f"\n\nTest failed with exit code: {return_code}\n")

    status = "PASSED" if success else "FAILED"
    print(f"\n{vendor.upper()} tests {status}. Log: {log_file}\n")
    return success, str(log_file)


def main():
    parser = argparse.ArgumentParser(
        description='Run Unity Catalog integration tests for AWS, Azure, and/or GCP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--aws', action='store_true', help='Run AWS tests')
    parser.add_argument('--azure', action='store_true', help='Run Azure tests')
    parser.add_argument('--gcp', action='store_true', help='Run GCP tests')
    parser.add_argument('--all', action='store_true', help='Run tests for all vendors')
    parser.add_argument('--build', action='store_true', help='Build project before testing')
    parser.add_argument('--skip-validation', action='store_true', help='Skip env validation')

    args = parser.parse_args()

    vendors = {'aws', 'azure', 'gcp'} if args.all else {
        v for v in ['aws', 'azure', 'gcp'] if getattr(args, v)
    }

    if not vendors:
        parser.print_help()
        print("\nERROR: No cloud vendor specified. Use --aws, --azure, --gcp, or --all\n")
        sys.exit(1)

    header("Unity Catalog Integration Test Runner")
    print(f"Selected vendors: {', '.join(v.upper() for v in sorted(vendors))}")

    if not validate_env(vendors, args.skip_validation):
        sys.exit(1)

    if args.build and not build_project():
        print("ERROR: Build failed. Aborting tests.\n")
        sys.exit(1)

    # Create logs directory and run tests
    log_dir = Path('tests/logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    print(f"Logs directory: {log_dir}/\n")

    results = {v: run_tests(v, log_dir) for v in sorted(vendors)}

    # Print summary
    header("Test Results Summary")
    for vendor in sorted(results):
        success, log_file = results[vendor]
        status = "PASSED" if success else "FAILED"
        print(f"  {vendor.upper()}: {status}")
        print(f"    Log: {log_file}")

    all_passed = all(success for success, _ in results.values())
    print(f"\n{'All tests passed!' if all_passed else 'Some tests failed!'}")

    if not all_passed:
        print("\nFailed test logs:")
        for vendor, (success, log_file) in results.items():
            if not success:
                print(f"  {vendor.upper()}: {log_file}")

    print()
    sys.exit(0 if all_passed else 1)


if __name__ == '__main__':
    main()
