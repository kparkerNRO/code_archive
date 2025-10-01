"""
A script to delete old versions of AWS Lambda functions. Unlike purge_lambda_versions.py,
this script explicitly deletes versions older than a certain threshold (default 10 days)
and keeps a specified number of the most recent versions (default 10).
"""

# Thanks to https://gist.github.com/tobywf/6eb494f4b46cef367540074512161334

from __future__ import absolute_import, print_function, unicode_literals
import boto3
import re
from datetime import datetime, timezone
import click

LAMBDA_PATTERN = r"^cmg-oasis-(dev|qa|prod)-leibniz-"
# LAMBDA_PATTERN = r"^cmg-oasis-(dev|qa|prod)-leibniz-nlp-socket-disconnect"  # for testing
LAMBDA_REGEX = re.compile(LAMBDA_PATTERN)


def get_all_versions(boto3_client, arn, is_layer=False):
    if is_layer:
        layer_name = arn.split(":layer:")[1].split(":")[0]
        paginator = boto3_client.get_paginator("list_layer_versions")
        response_iterator = paginator.paginate(LayerName=layer_name)
    else:
        paginator = boto3_client.get_paginator("list_versions_by_function")
        response_iterator = paginator.paginate(FunctionName=arn)

    versions = []
    for page in response_iterator:
        versions.extend(page["LayerVersions" if is_layer else "Versions"])
    return versions


def delete_old_versions(
    arn,
    age_threshold_days=20,
    versions_to_keep=20,
    should_execute=False,
    is_layer=False,
):
    client = boto3.client("lambda")
    versions = get_all_versions(client, arn, is_layer)
    resource_type = "layer" if is_layer else "function"

    if not is_layer:
        # versions don't list code size, only functions do
        total_storage = sum(version.get("CodeSize", 0) for version in versions)
        print(f"Total {resource_type} storage used: {total_storage / (1024 * 1024):.2f} MB")

    versions.sort(key=lambda x: x["Version"], reverse=True)
    old_versions = versions[versions_to_keep:]

    now = datetime.now(timezone.utc)
    if is_layer:
        old_versions = [
            v
            for v in old_versions
            if (now - datetime.strptime(v["CreatedDate"], "%Y-%m-%dT%H:%M:%S.%f%z")).days > age_threshold_days
        ]
    else:
        old_versions = [
            v
            for v in old_versions
            if "LastModified" in v
            and (now - datetime.strptime(v["LastModified"], "%Y-%m-%dT%H:%M:%S.%f%z")).days > age_threshold_days
            and v["Version"] != "$LATEST"
        ]

    print(f"Found {len(old_versions)} out of {len(versions)} {resource_type} versions to delete.")

    if not is_layer:
        total_storage = sum(version.get("CodeSize", 0) for version in old_versions)
        print(f"Total {resource_type} storage to delete: {total_storage / (1024 * 1024):.2f} MB")

    deleted_count = 0
    for version in old_versions:
        version_id = version["Version"]

        if not is_layer:
            aliases = client.list_aliases(FunctionName=arn, FunctionVersion=version_id)["Aliases"]
            if aliases:
                print(f"Skipping version due to {len(aliases)} active aliases {arn=}")
                continue

        if not should_execute:
            deleted_count += 1
            continue

        deleted_count += 1
        if is_layer:
            layer_name = arn.split(":layer:")[1].split(":")[0]
            client.delete_layer_version(LayerName=layer_name, VersionNumber=version_id)
        else:
            client.delete_function(FunctionName=arn, Qualifier=version_id)
        print(f"Deleted {resource_type} version {version_id}")

    return deleted_count


def clean_old_lambda_versions(
    lambda_regex,
    age_threshold_days=20,
    versions_to_keep=20,
    should_execute=False,
):
    deleted_count = 0
    client = boto3.client("lambda")
    functions = client.list_functions()["Functions"]
    valid_functions = [function for function in functions if lambda_regex.match(function["FunctionName"])]
    for function in valid_functions:
        function_name = function["FunctionName"]
        lambda_arn = function["FunctionArn"]

        print()
        print(f"Processing {function_name=}.")

        deleted_count += delete_old_versions(
            lambda_arn,
            age_threshold_days=age_threshold_days,
            versions_to_keep=versions_to_keep,
            should_execute=should_execute,
            is_layer=False,
        )

    layers = client.list_layers()["Layers"]
    valid_layers = [layer for layer in layers if lambda_regex.match(layer["LayerName"])]
    for layer in valid_layers:
        layer_name = layer["LayerName"]
        layer_arn = layer["LayerArn"]

        if not lambda_regex.match(layer_name):
            print(f"Layer name '{layer_name}' does not match {LAMBDA_PATTERN}.")
            continue

        print()
        print(f"Processing {layer_name=}.")

        deleted_count += delete_old_versions(
            layer_arn,
            age_threshold_days=age_threshold_days,
            versions_to_keep=versions_to_keep,
            should_execute=should_execute,
            is_layer=True,
        )

    return deleted_count


@click.command()
@click.argument(
    "arn",
    required=False,
    default=None,
)
@click.option(
    "-e",
    "--execute",
    is_flag=True,
    default=False,
    help="Execute the deletion (without this flag, only shows what would be deleted)",
)
@click.option("-d", "--days", required=False, default=10, help="Minimum number of days to keep")
@click.option("-r", "--records", required=False, default=10, help="Minimum number of records to keep")
def main(arn, execute, days, records):
    if arn:
        is_layer = ":layer:" in arn

        records_deleted = delete_old_versions(
            arn,
            should_execute=execute,
            age_threshold_days=days,
            versions_to_keep=records,
            is_layer=is_layer,
        )
    else:
        records_deleted = clean_old_lambda_versions(
            LAMBDA_REGEX,
            should_execute=execute,
            age_threshold_days=days,
            versions_to_keep=records,
        )

    print()
    if not execute:
        print(f"Would have deleted {records_deleted} total records")
    else:
        print(f"Deleted {records_deleted} total records")


if __name__ == "__main__":
    main()
