#!/usr/bin/env python3
import aws_cdk as cdk
from agent_companion.stack import AgentCompanionServiceStack
from aws_cdk import Tags
from core.stack import HopperCoreStack
from data_pipeline.stack import AEDetectionsPipelineStack
from settings import Environment, HopperConfig

app = cdk.App()

env_name = app.node.try_get_context("env") or "dev"
env_config = app.node.try_get_context("environments")[env_name]

if not env_config:
    raise ValueError(f"Environment '{env_name}' not found in cdk.json context")

context = app.node.get_all_context()
config = HopperConfig.from_config(
    context,
    Environment(env_name),
)

account = config.environment_config.account
region = config.environment_config.region

if config.is_slot and env_name != "dev":
    raise ValueError("PR slot requested, but not in dev environment")

if not config.is_slot:
    core_stack = HopperCoreStack(
        app,
        "hopper-core",
        env=cdk.Environment(account=account, region=region),
        config=config,
    )

    pipeline = AEDetectionsPipelineStack(
        app,
        "hopper-data-pipeline",
        env=cdk.Environment(account=account, region=region),
        config=config,
    )
    pipeline.add_dependency(core_stack)

    app_stack = AgentCompanionServiceStack(
        app,
        "hopper-agent-companion",
        env=cdk.Environment(account=account, region=region),
        config=config,
    )
    app_stack.add_dependency(core_stack)
else:
    # Core stack and app stack are not currently deployed in slot deployments
    core_stack = None
    app_stack = None

    pipeline = AEDetectionsPipelineStack(
        app,
        f"hopper-data-pipeline-{config.dev_slot}",
        env=cdk.Environment(account=account, region=region),
        config=config,
    )

# Add tags to all deployed stacks
for stack in (core_stack, pipeline, app_stack):
    if not stack:
        continue
    for key, value in config.resource_tags.items():
        Tags.of(stack).add(key, value)

app.synth()
