#!/usr/bin/env python
# /// script
# dependencies = [
#   "click~=8.1.8",
#   "PyYAML"
# ]
# ///

"""
It remains a delightful feature of AWS that every service has its own ideosyncratic syntax,
even for nominally identical things such as SAM and Cloudformation deploy commands. This script aims to
abstract that mess away for parameter definititions, and adds a few quality-of-life features on top

The script supports up to three sources of parameters, evaluated in this order:
1. A default/base settings file
2. A per-envionment settings file
3. Command line overrides

The script will merge these three sources of parameters, in that order, and output
the result in the specified format. 

The script supports three output formats:
1. SAM config (a samconfig.yaml file using the standard SAM config format)
2. CloudFormation parameters (a JSON file using the standard CloudFormation parameter format)
3. Command-line key/value pairs (a text file with key/value pairs for use in command-line arguments)

This script is set up to be run by uv. No virtual environment is needed, the script section at the
top defines the script-specific dependencies

Example usage:
uv run --no-project \
    infrastructure/scripts/generate_settings.py \
    infrastructure/envs/dev.yaml \
    -d infrastructure/envs/defaults.yaml \
    -o infrastructure/lambda/sam_config.yaml \
    -f sam \
    -v Env dev

uv run scripts/generate_settings.py envs/dev.yaml -f cfn
"""

from io import TextIOWrapper
import json
from pathlib import Path
import click
import yaml
from enum import Enum


class OutputFormat(Enum):
    SAM = "sam"
    CFN = "cfn"
    CMD = "cmd"


def parse_sam(data, environment, output_file):
    cfg_values = [f"ParameterKey={key},ParameterValue={value}" for key, value in data.items()]
    sam_config = {"version": 0.1, environment: {"deploy": {"parameters": {"parameter_overrides": cfg_values}}}}

    output = yaml.dump(sam_config, default_flow_style=False, sort_keys=False)
    with open(output_file, "w") as f:
        f.write(output)


def parse_cfn(data, output_file):
    cf_params = [f"{key}={value}" for key, value in data.items()]
    output = json.dumps(cf_params, indent=2)
    with open(output_file, "w") as f:
        f.write(output)


def parse_cmd(data, output_file):
    output = " ".join([f"{key}={value}" for key, value in data.items()])
    with open(output_file, "w") as f:
        f.write(output)


def run_convert(
    input_file: TextIOWrapper,
    format: OutputFormat,
    defaults_file: Path = None,
    environment: str = "dev",
    overrides: list = [],
    output_file: Path = None,
):
    """
    Convert a JSON file of key/value pairs into the specified format: SAM config, CloudFormation parameters, or command-line key/value pairs.
    """
    data = {}
    # load in the data
    if defaults_file:
        with open(defaults_file, "r") as f:
            input_data = yaml.safe_load(f)
            data.update(input_data)

    input_data = yaml.safe_load(input_file)
    data.update(input_data)

    if overrides:
        for key, value in overrides:
            data[key] = value

    # convert values to the correct format
    for key, value in data.items():
        if isinstance(value, list):
            data[key] = ",".join(value)

    output_format = OutputFormat(format)

    if output_format == OutputFormat.SAM:
        output_file = output_file or "sam_config.yaml"
        parse_sam(data, environment, output_file)
    elif output_format == OutputFormat.CFN:
        output_file = output_file or "cloudformation_parameters.json"
        parse_cfn(data, output_file)
    elif output_format == OutputFormat.CMD:
        output_file = output_file or "overrides.txt"
        parse_cmd(data, output_file)


@click.command()
@click.argument("input_file", type=click.File("r"))
@click.option(
    "--format",
    "-f",
    required=True,
    type=click.Choice([fmt.value for fmt in OutputFormat]),
    help='Output format: "sam" for SAM config, "cfn" for CloudFormation parameters, "cmd" for command-line key/value pairs',
)
@click.option(
    "--defaults-file",
    "-d",
    required=False,
    type=click.Path(exists=True),
    help="Defaults JSON file containing key/value pairs to be overridden",
)
@click.option("--environment", "-e", default="dev", help="Environment name for SAM configuration (default: dev)")
@click.option("--output-file", "-o", default=None, help="Optional output file name")
@click.option(
    "--overrides",
    "-v",
    multiple=True,
    type=(str, str),
    help="Override key/value pairs to include in the output (e.g., -v key1 value1 -v key2 value2)",
)
def convert(input_file, format, defaults_file, environment, output_file, overrides):
    """
    Convert a JSON file of key/value pairs into the specified format: SAM config, CloudFormation parameters, or command-line key/value pairs.
    """
    run_convert(
        input_file=input_file,
        format=format,
        defaults_file=defaults_file,
        environment=environment,
        output_file=output_file,
        overrides=overrides,
    )


if __name__ == "__main__":
    convert()
