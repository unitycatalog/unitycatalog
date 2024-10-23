import argparse
import sys

from databricks.sdk import WorkspaceClient


def parse_args(args):
    parser = argparse.ArgumentParser(description="Clean up functions")
    parser.add_argument(
        "--catalog",
        required=True,
        help="The catalog to clean up functions from",
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="The schema to clean up functions from",
    )
    return parser.parse_args(args)


def cleanup_functions(args):
    args = parse_args(args)
    client = WorkspaceClient()
    failed_deletions = {}
    function_infos = client.functions.list(
        catalog_name=args.catalog, schema_name=args.schema
    )
    for function_info in function_infos:
        try:
            client.functions.delete(function_info.full_name)
        except Exception as e:
            failed_deletions[function_info.full_name] = str(e)

    if failed_deletions:
        sys.stderr.write(
            f"Failed to delete the following functions: {failed_deletions}"
        )
        sys.exit(1)


if __name__ == "__main__":
    cleanup_functions(sys.argv[1:])
