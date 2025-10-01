import json
import os
from dataclasses import dataclass
from typing import Any

from aws_cdk import (
    BundlingOptions,
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    Tags,
    aws_apigateway as apigw,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as eventsrc,
    aws_logs as logs,
    aws_s3 as s3,
    aws_servicediscovery as servicediscovery,
    aws_sqs as sqs,
)
from constructs import Construct
from settings import RESOURCE_NAME_TAG_PREFIX, Environment, HopperConfig


@dataclass
class LambdaParams:
    function_name_override: str | None = None


@dataclass(kw_only=True)
class DockerParams(LambdaParams):
    ecr_repository: str
    ecr_image_tag: str = "latest"


@dataclass(kw_only=True)
class ZipParams(LambdaParams):
    lambda_layers: list
    code_asset: Any


class BridgeLambdaConstruct(Construct):
    """
    A construct for creating a Lambda function with optional EventBridge trigger.

    This makes the assumption that "function_id" is a string that can be split into words
    to create appropriately cased names for the construct and the Lambda function, for the
    function name, and for the handler, which is assumed to have the handler "function_id.handler".
    """

    def __init__(
        self,
        scope,
        function_id,
        name_prefix,
        name_prefix_short,
        env_vars,
        vpc,
        vpc_subnets,
        security_groups,
        lambda_execution_role,
        lambda_params: LambdaParams,
        event_bridge=None,
        *,  # make the rest of these mandatory kwargs
        sqs_patterns=None,
        event_patterns=None,
        event_schedule=None,
        max_concurrent_lambdas=15,
        message_batch_size=5,
    ):
        words = function_id.lower().split()
        cfn_id = "".join(word.capitalize() for word in words)
        function_name = f"{name_prefix}-{'-'.join(words)}"
        function_name_short = f"{name_prefix_short}-{'-'.join(words)}"
        handler = f"data_pipeline.{'_'.join(words)}.handler"

        self.event_identifier = f"data-pipeline.{function_name}"
        env_vars_with_id = env_vars | {"EVENT_IDENTIFIER": self.event_identifier}

        super().__init__(scope, cfn_id)

        # This is an ugly pattern, but because CDK depends so heavily on init methods, using inheritence
        # was largely out. This was the best way to both enforce standard parameters and reuse code between largely
        # identical definitions
        params = dict(
            memory_size=256,
            architecture=lambda_.Architecture.X86_64,
            environment=env_vars_with_id,
            role=lambda_execution_role,
            vpc=vpc,
            vpc_subnets=vpc_subnets,
            security_groups=security_groups,
            timeout=Duration.minutes(10),
        )
        if isinstance(lambda_params, DockerParams):
            function_name = (
                lambda_params.function_name_override
                if lambda_params.function_name_override is not None
                else f"{function_name}-docker"
            )
            self.function = lambda_.DockerImageFunction(
                self,
                f"{cfn_id}Function",
                function_name=function_name,
                code=lambda_.DockerImageCode.from_ecr(
                    repository=lambda_params.ecr_repository,
                    tag_or_digest=lambda_params.ecr_image_tag,
                    cmd=[handler],
                ),
                **params,
            )
        elif isinstance(lambda_params, ZipParams):
            function_name = (
                lambda_params.function_name_override
                if lambda_params.function_name_override is not None
                else f"{function_name}-zip"
            )
            self.function = lambda_.Function(
                self,
                f"{cfn_id}Function",
                function_name=function_name,
                runtime=lambda_.Runtime.PYTHON_3_12,
                code=lambda_params.code_asset,
                handler=handler,
                layers=lambda_params.lambda_layers,
                **params,
            )
        else:
            raise ValueError("Lambda params must be ZipParams or DockerParams")

        Tags.of(self.function).add(RESOURCE_NAME_TAG_PREFIX, function_name)

        # Set up CloudWatch logs and metrics
        log_group = logs.LogGroup(
            self,
            f"{cfn_id}LogGroup",
            log_group_name=f"/aws/lambda/{function_name}",
            retention=logs.RetentionDays.FIVE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        )

        # Create a metric filter to track errors in the logs
        log_group.add_metric_filter(
            f"{cfn_id}ErrorMetricFilter",
            filter_pattern=logs.FilterPattern.any_term("ERROR", "error", "Error"),
            metric_name=f"{function_name_short}-errors",
            metric_namespace="AriaPipeline",
            default_value=0,
            metric_value="1",
        )

        self.event_bridge = event_bridge

        dlq_name = f"{function_name}-dlq"
        dlq = sqs.Queue(
            self,
            f"{cfn_id}DLQ",
            queue_name=dlq_name,
            retention_period=Duration.days(14),
        )
        Tags.of(dlq).add(RESOURCE_NAME_TAG_PREFIX, dlq_name)

        event_rules = []
        if event_bridge and event_patterns:
            for i, pattern in enumerate(event_patterns):
                event_rules.append(
                    events.Rule(
                        self,
                        f"{cfn_id}Rule{i}",
                        rule_name=f"{function_name_short}-rule-{i}",
                        event_bus=event_bridge,
                        event_pattern=pattern,
                    )
                )

        if event_schedule:
            event_rules.append(
                events.Rule(self, f"{cfn_id}Rule", rule_name=f"{function_name_short}-sched", schedule=event_schedule)
            )

        if event_rules:
            self._init_event_bridge_rules(cfn_id, self.function.function_arn, event_rules, dlq)

        # Set up SQS pattern(s)
        if event_bridge and sqs_patterns:
            sqs_queue = self._init_sqs_connections(cfn_id, function_name_short, sqs_patterns, dlq)

            # Add SQS as event source for Lambda
            # NB: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
            # "If you're using a batch window and your SQS queue contains very low traffic,
            # Lambda might wait for up to 20 seconds before invoking your function.
            # This is true even if you set a batch window lower than 20 seconds"
            self.function.add_event_source(
                eventsrc.SqsEventSource(
                    queue=sqs_queue,
                    batch_size=message_batch_size,
                    max_batching_window=Duration.seconds(10),
                    report_batch_item_failures=True,
                    max_concurrency=max_concurrent_lambdas,
                )
            )

            lambda_execution_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                    ],
                    resources=[sqs_queue.queue_arn],
                )
            )

    def _init_event_bridge_rules(self, cfn_id, function_arn, event_rules, dlq):
        # Create an execution role for the EventBridge target
        target_execution_role = iam.Role(
            self,
            f"{cfn_id}TargetExecutionRole",
            assumed_by=iam.ServicePrincipal("events.amazonaws.com"),
        )

        target_execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[function_arn],
            )
        )

        # Add the Lambda function as a target with retries and DLQ
        for rule in event_rules:
            rule.add_target(
                targets.LambdaFunction(
                    handler=self.function,
                    event=events.RuleTargetInput.from_event_path("$"),
                    retry_attempts=2,
                    max_event_age=Duration.hours(2),
                    dead_letter_queue=dlq,
                )
            )

    def _init_sqs_connections(self, cfn_id, function_name, sqs_patterns, dlq):
        # Create the main queue with DLQ enabled
        queue_name = f"{function_name}-queue"
        queue = sqs.Queue(
            self,
            f"{cfn_id}Queue",
            queue_name=queue_name,
            visibility_timeout=Duration.minutes(10),  # Should be longer than lambda timeout
            retention_period=Duration.days(14),
            dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=5),
        )

        Tags.of(queue).add(RESOURCE_NAME_TAG_PREFIX, queue_name)

        queue.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal("events.amazonaws.com")],
                actions=["sqs:SendMessage"],
                resources=[queue.queue_arn],
            )
        )

        # Create EventBridge rules for each pattern and add SQS targets
        for i, pattern in enumerate(sqs_patterns):
            rule = events.Rule(
                self,
                f"{cfn_id}SqsRule{i}",
                rule_name=f"{function_name}-sqs-{i}",
                event_bus=self.event_bridge,
                event_pattern=pattern,
            )

            rule.add_target(
                targets.SqsQueue(
                    queue=queue,
                    message=events.RuleTargetInput.from_event_path("$"),
                    dead_letter_queue=dlq,
                )
            )

        return queue


class AEDetectionsPipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        config: HopperConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        name_prefix = config.name_prefix
        name_prefix_for_imports = config.name_prefix_import

        vpc = config.environment_config.vpc(self)
        private_subnet_selection = config.environment_config.private_subnet_selection(self)

        rds_secret_name = Fn.import_value(f"{name_prefix_for_imports}-DbCredentialsSecretName")
        rds_security_group_id = Fn.import_value(f"{name_prefix_for_imports}-RDSSecurityGroupId")

        encryption_key_secret_name = Fn.import_value(f"{name_prefix_for_imports}-DataEncryptionKeySecretName")

        # Create resource references
        ecs_cluster_name = Fn.import_value(f"{name_prefix_for_imports}-EcsClusterName")
        ecs_cluster = ecs.Cluster.from_cluster_attributes(
            self,
            "EcsCluster",
            cluster_name=ecs_cluster_name,
            vpc=vpc,
        )

        Tags.of(ecs_cluster).add(RESOURCE_NAME_TAG_PREFIX, ecs_cluster_name)

        cluster_namespace_name = Fn.import_value(f"{name_prefix_for_imports}-ClusterNamespaceName")
        cluster_namespace_arn = Fn.import_value(f"{name_prefix_for_imports}-ClusterNamespaceArn")
        cluster_namespace_id = Fn.import_value(f"{name_prefix_for_imports}-ClusterNamespaceId")
        cluster_namespace = servicediscovery.PrivateDnsNamespace.from_private_dns_namespace_attributes(
            self,
            "ClusterNamespace",
            namespace_arn=cluster_namespace_arn,
            namespace_id=cluster_namespace_id,
            namespace_name=cluster_namespace_name,
        )

        event_bus_arn = Fn.import_value(f"{name_prefix_for_imports}-EventBusArn")
        event_bus = events.EventBus.from_event_bus_arn(self, "ExistingEventBus", event_bus_arn=event_bus_arn)

        pipeline_bucket_arn = Fn.import_value(f"{name_prefix_for_imports}-PipelineBucketArn")

        # common security group configuration
        pipeline_security_group = ec2.SecurityGroup(
            self,
            "PipelineSecurityGroup",
            vpc=vpc,
            description="Security group for communication between services",
            allow_all_outbound=True,
        )
        pipeline_security_group.add_ingress_rule(
            peer=pipeline_security_group,
            connection=ec2.Port.all_traffic(),
            description="Allow all traffic within the security group",
        )

        rds_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "RdsSecurityGroup",
            security_group_id=rds_security_group_id,
        )
        rds_security_group.add_ingress_rule(
            pipeline_security_group, ec2.Port.tcp(5432), "Allow PostgreSQL access from ECS tasks"
        )

        ##############################
        # AE detection service ECS
        ##############################
        ae_detection_service = self._init_ae_detection_service(
            config,
            vpc,
            private_subnet_selection,
            ecs_cluster,
            cluster_namespace,
            pipeline_security_group,
        )

        ##############################
        # Lambda configuration
        ##############################
        pipeline_bucket = s3.Bucket.from_bucket_arn(self, "PipelineBucket", pipeline_bucket_arn)
        submit_call_lambda = self._init_lambdas(
            config,
            pipeline_bucket,
            vpc,
            event_bus,
            rds_secret_name,
            encryption_key_secret_name,
            ae_detection_service.cloud_map_service.service_name,
            cluster_namespace.namespace_name,
            private_subnet_selection,
            pipeline_security_group,
        )

        ##############################
        # REST API Gateway configuration
        ##############################
        rest_api = self._init_apigateway(config, submit_call_lambda)

        ##############################
        # Outputs
        ##############################
        CfnOutput(
            self,
            "SubmitCallEndpoint",
            value=f"https://{rest_api.rest_api_id}.execute-api.{self.region}.amazonaws.com/api/submit-call",
            description="URL for the submit-call endpoint",
            export_name=f"{name_prefix}-SubmitCallEndpoint",
        )

    def _init_ae_detection_service(
        self,
        config: HopperConfig,
        vpc,
        private_subnet_selection,
        ecs_cluster,
        cluster_namespace,
        pipeline_security_group,
    ):
        name_prefix = config.name_prefix
        ae_detection_repo = config.environment_config.ae_detection_repo
        ae_detection_arn = config.environment_config.ae_detection_arn

        # security groups and IAM roles
        detection_service_security_group = ec2.SecurityGroup(
            self,
            "DetectionServiceSecurityGroup",
            vpc=vpc,
            description="Security group for the AE detection service",
            allow_all_outbound=True,
        )

        detection_service_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(8080),
            "Allow inbound HTTP traffic on port 8080",
        )

        deployment_role = iam.Role(
            self,
            "AEDPipelineDeploymentRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
            role_name=f"{name_prefix}-detection-deployment-role",
        )

        deployment_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:DescribeRepositories",
                    "ecr:BatchGetImage",
                ],
                resources=[ae_detection_arn],
            )
        )

        task_role = iam.Role(
            self,
            "AEDPipelineTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
            role_name=f"{name_prefix}-detection-task-role",
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["logs:CreateLogGroup"],
                resources=[
                    f"arn:aws:logs:{config.environment_config.region}:{config.environment_config.account}:log-group:*"
                ],
            )
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModelWithResponseStream",
                    "bedrock:InvokeModel",
                    "bedrock:ListFoundationModels",
                    "bedrock:GetModelInvocationLoggingConfiguration",
                ],
                resources=["*"],
            )
        )

        # Task and service definitions
        task_definition = ecs.FargateTaskDefinition(
            self,
            "AEDetectionServiceTaskDefinition",
            cpu=8192,  # 8 vCPU
            memory_limit_mib=32768,  # 32GB
            execution_role=deployment_role,
            task_role=task_role,
            family=f"{name_prefix}-detection-task-def",
        )

        # Set up CloudWatch alarm for service errors
        ae_detection_logs = logs.LogGroup(
            self,
            "AEDetectionLogs",
            log_group_name=f"/ecs/{name_prefix}-ae-detection",
            retention=logs.RetentionDays.FIVE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        )

        task_definition.add_container(
            "WebContainer",
            image=ecs.ContainerImage.from_registry(ae_detection_repo),
            logging=ecs.LogDriver.aws_logs(stream_prefix=f"{name_prefix}-ae-detection", log_group=ae_detection_logs),
            port_mappings=[
                ecs.PortMapping(
                    container_port=8080,
                    protocol=ecs.Protocol.TCP,
                    name="web-container",
                ),
            ],
            environment={
                "PORT": "8080",
                "WORKERS": "10",
            },
            health_check=ecs.HealthCheck(
                command=["CMD-SHELL", "curl -f http://localhost:8080/health || exit 1"],
                interval=Duration.seconds(60),
                timeout=Duration.seconds(5),
                retries=3,
                start_period=Duration.seconds(60),
            ),
        )

        ae_detection_service_name = f"{name_prefix}-ae-pipeline-service"
        ae_detection_service = ecs.FargateService(
            self,
            "AEPipelineService",
            service_name=ae_detection_service_name,
            cluster=ecs_cluster,
            task_definition=task_definition,
            desired_count=1,
            vpc_subnets=private_subnet_selection,
            assign_public_ip=False,
            security_groups=[
                pipeline_security_group,
                detection_service_security_group,
            ],
            cloud_map_options=ecs.CloudMapOptions(
                cloud_map_namespace=cluster_namespace,
                name=f"{name_prefix}-ae-detection-service",
                dns_record_type=servicediscovery.DnsRecordType.A,
            ),
            min_healthy_percent=100,
            health_check_grace_period=Duration.minutes(5),
            circuit_breaker=ecs.DeploymentCircuitBreaker(
                enable=True,
                rollback=True,
            ),
        )

        ae_detection_logs.add_metric_filter(
            "AEDetectionErrorMetricFilter",
            filter_pattern=logs.FilterPattern.any_term("error", "ERROR", "Error", "500"),
            metric_namespace="AriaPipeline",
            metric_name=f"{name_prefix}-ae-detection-errors",
            default_value=0,
            metric_value="1",
        )

        Tags.of(ae_detection_service).add(RESOURCE_NAME_TAG_PREFIX, ae_detection_service_name)

        return ae_detection_service

    def _init_zip_lambdas(self, name_prefix):
        # Lambda layer
        # pydantic is really, well, pedantic, about how it's installed.
        # This is the least painful way to get it to work
        # https://docs.pydantic.dev/latest/integrations/aws_lambda/#installing-pydantic-for-aws-lambda-functions
        dependencies_layer = lambda_.LayerVersion(
            self,
            "DataPipelineDependenciesLayer",
            layer_version_name=f"{name_prefix}-data-pipeline-dependencies",
            code=lambda_.Code.from_asset(
                path=os.path.join(os.path.dirname(__file__), "../../../backend/"),
                bundling=BundlingOptions(
                    image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install -r data_pipeline/requirements.txt "
                        "--platform manylinux_2_17_x86_64 --only-binary=:all: "
                        "--target /asset-output/python",
                    ],
                ),
            ),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            compatible_architectures=[lambda_.Architecture.X86_64],
            description="Dependencies for data pipeline Lambda functions",
            removal_policy=RemovalPolicy.DESTROY,
        )

        code_asset = lambda_.Code.from_asset(
            path=os.path.join(os.path.dirname(__file__), "../../../backend/"),
            bundling=BundlingOptions(
                image=lambda_.Runtime.PYTHON_3_12.bundling_image,
                command=[
                    "bash",
                    "-c",
                    "cp data_pipeline/requirements.txt /asset-output/ "
                    "&& cp -r common/common /asset-output/ "
                    "&& cp -r data_pipeline/data_pipeline /asset-output/",
                ],
            ),
        )

        zip_params = ZipParams(lambda_layers=[dependencies_layer], code_asset=code_asset)

        return zip_params

    def _init_lambdas(
        self,
        config: HopperConfig,
        pipeline_bucket,
        vpc,
        event_bus,
        rds_secret_name,
        encryption_key_secret_name,
        ae_detection_service_name,
        cluster_namespace_name,
        private_subnet_selection,
        pipeline_security_group,
    ):
        name_prefix = config.name_prefix
        name_prefix_for_imports = config.name_prefix_import
        name_prefix_short = config.name_prefix_short

        # Permissions
        policies = [
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[pipeline_bucket.bucket_arn, pipeline_bucket.bucket_arn + "/*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:DeleteObject"],
                resources=[pipeline_bucket.bucket_arn + "/calls/*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{rds_secret_name}*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{encryption_key_secret_name}*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "kms:Decrypt",
                    "kms:Encrypt",
                    "kms:DescribeKey",
                    "kms:GenerateDataKey",
                    "kms:GenerateDataKeyWithoutPlaintext",
                    "kms:GenerateDataKeyPair",
                    "kms:GenerateDataKeyPairWithoutPlaintext",
                    "kms:CreateGrant",
                    "kms:ListAliases",
                    "kms:ListKeys",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[config.environment_config.talkdesk_secret_arn + "*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[config.environment_config.talkdesk_secret_arn + "*"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["events:PutEvents", "events:GetEventBus"],
                resources=[event_bus.event_bus_arn],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["events:PutEvents", "events:GetEventBus"],
                resources=[f"arn:aws:events:{self.region}:{self.account}:event-bus/default"],
            ),
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:cmg-hopper-dev-talkdesk-call-fetcher"
                ],
            ),
        ]

        lambda_execution_role = iam.Role(
            self,
            "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            role_name=f"{name_prefix}-lambda-execution-role",
        )

        lambda_execution_role.add_managed_policy(
            iam.ManagedPolicy.from_managed_policy_arn(
                self,
                "BasicExecutionPolicy",
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
            )
        )

        lambda_execution_role.add_managed_policy(
            iam.ManagedPolicy.from_managed_policy_arn(
                self,
                "VPCAccessPolicy",
                "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole",
            )
        )

        for policy in policies:
            lambda_execution_role.add_to_policy(policy)
        audio_processing_execution_role = lambda_execution_role
        audio_processing_execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[config.environment_config.deepgram_secret_arn],
            )
        )

        # Lambda definitions
        common_event_vars = {
            "EVENT_BUS_NAME": event_bus.event_bus_name,
            "RDS_SECRET_NAME": rds_secret_name,
            "ENCRYPTION_KEY_SECRET_NAME": encryption_key_secret_name,
            "PIPELINE_BUCKET_NAME": pipeline_bucket.bucket_name,
        }

        pipeline_repository_name = Fn.import_value(f"{name_prefix_for_imports}-PipelineRepositoryName")
        pipeline_repository_arn = f"arn:aws:ecr:{self.region}:{self.account}:repository/{pipeline_repository_name}"
        pipeline_repository = ecr.Repository.from_repository_attributes(
            self,
            "PipelineRepository",
            repository_arn=pipeline_repository_arn,
            repository_name=pipeline_repository_name,
        )
        pipeline_repository.grant_pull(lambda_execution_role)

        docker_params = DockerParams(
            ecr_repository=pipeline_repository,
            ecr_image_tag=config.docker_tag,
        )
        base_params = docker_params

        if config.environment in (Environment.DEV, Environment.QA):
            # Zip file (editable) lambda for running test code in AWS Lambdas
            # This is primarily a debugging tool; it creates a lambda where developers can edit code
            # that is configured identically to the docker containers

            zip_params = self._init_zip_lambdas(name_prefix)
            base_params = docker_params if not config.is_slot else zip_params

            BridgeLambdaConstruct(
                self,
                function_id="lambda test",
                name_prefix=name_prefix,
                name_prefix_short=name_prefix_short,
                env_vars=common_event_vars,
                lambda_execution_role=lambda_execution_role,
                vpc=vpc,
                vpc_subnets=private_subnet_selection,
                security_groups=[pipeline_security_group],
                lambda_params=ZipParams(
                    lambda_layers=zip_params.lambda_layers,
                    code_asset=zip_params.code_asset,
                    # override the function name to maintain the existing lambda name
                    function_name_override=f"{name_prefix}-utils",
                ),
            )

        # Submit Call Lambda function for HTTP API
        submit_call_lambda = BridgeLambdaConstruct(
            self,
            function_id="submit call",
            name_prefix=name_prefix,
            name_prefix_short=name_prefix_short,
            env_vars=common_event_vars,
            lambda_execution_role=lambda_execution_role,
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            security_groups=[pipeline_security_group],
            lambda_params=base_params,
        )

        # Talkdesk Download Lambda function
        td_download = BridgeLambdaConstruct(
            self,
            function_id="talkdesk download",
            name_prefix=name_prefix,
            name_prefix_short=name_prefix_short,
            env_vars=common_event_vars
            | {
                "TALKDESK_SECRET_ARN": config.environment_config.talkdesk_secret_arn,
                "TALKDESK_API_URL": config.environment_config.talkdesk_api_url,
                "S3_BUCKET": pipeline_bucket.bucket_name,
            },
            lambda_execution_role=lambda_execution_role,
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            security_groups=[pipeline_security_group],
            lambda_params=base_params,
            event_bridge=event_bus,
            sqs_patterns=[{"source": [submit_call_lambda.event_identifier]}],
        )

        # Audio Processing Lambda function
        audio_processing = BridgeLambdaConstruct(
            self,
            function_id="audio processing",
            name_prefix=name_prefix,
            name_prefix_short=name_prefix_short,
            env_vars=common_event_vars
            | {
                "DEEPGRAM_API_SECRET_NAME": config.environment_config.deepgram_secret_arn,
            },
            lambda_execution_role=audio_processing_execution_role,
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            security_groups=[pipeline_security_group],
            lambda_params=base_params,
            event_bridge=event_bus,
            sqs_patterns=[{"source": [td_download.event_identifier]}],
        )

        # AE Detection Interface Lambda function
        BridgeLambdaConstruct(
            self,
            function_id="ae detection",
            name_prefix=name_prefix,
            name_prefix_short=name_prefix_short,
            env_vars=common_event_vars
            | {
                "AE_DETECTION_SERVICE_URL": f"{ae_detection_service_name}.{cluster_namespace_name}:8080",  # noqa: E501
            },
            lambda_execution_role=lambda_execution_role,
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            security_groups=[pipeline_security_group],
            lambda_params=base_params,
            event_bridge=event_bus,
            sqs_patterns=[{"source": [audio_processing.event_identifier]}],
            max_concurrent_lambdas=2,
        )

        # Talkdesk user sync lambda
        BridgeLambdaConstruct(
            self,
            function_id="talkdesk sync users",
            name_prefix=name_prefix,
            name_prefix_short=name_prefix_short,
            env_vars=common_event_vars
            | {
                "TALKDESK_SECRET_ARN": config.environment_config.talkdesk_secret_arn,
                "TALKDESK_API_URL": config.environment_config.talkdesk_api_url,
                "S3_BUCKET": pipeline_bucket.bucket_name,
                "CALL_CENTER_EXTERNAL_ID": "ASO PACT/TalkDesk",
            },
            lambda_execution_role=lambda_execution_role,
            vpc=vpc,
            vpc_subnets=private_subnet_selection,
            security_groups=[pipeline_security_group],
            lambda_params=base_params,
            event_bridge=event_bus,
            event_schedule=events.Schedule.cron(minute="0", hour="8", month="*", week_day="MON-FRI", year="*"),
        )

        return submit_call_lambda

    def _init_apigateway(self, config, submit_call_lambda):
        name_prefix = config.name_prefix

        # Roche only supports REST API GWs, so that's what we're using
        # https://sites.google.com/roche.com/rcp/aws/security-boundaries/securely-accessing-api-gw-from-rcn-thought-vpce
        rest_api_name = f"{name_prefix}-callback-api"
        rest_api = apigw.RestApi(
            self,
            "CallWebhookRestApi",
            rest_api_name=rest_api_name,
            description="REST API for call webhook endpoints",
            deploy_options=apigw.StageOptions(
                stage_name="api",
                logging_level=apigw.MethodLoggingLevel.INFO,
                data_trace_enabled=True,
                metrics_enabled=True,
                access_log_destination=apigw.LogGroupLogDestination(
                    logs.LogGroup(
                        self,
                        "ApiGatewayAccessLogs",
                        log_group_name=f"/aws/apigateway/{name_prefix}-callback-api-access-logs",
                        retention=logs.RetentionDays.ONE_WEEK,
                        removal_policy=RemovalPolicy.DESTROY,
                    )
                ),
                access_log_format=apigw.AccessLogFormat.custom(
                    json.dumps(
                        {
                            "requestId": "$context.requestId",
                            "ip": "$context.identity.sourceIp",
                            "requestTime": "$context.requestTime",
                            "httpMethod": "$context.httpMethod",
                            "routeKey": "$context.routeKey",
                            "status": "$context.status",
                            "protocol": "$context.protocol",
                            "responseLength": "$context.responseLength",
                            "extendedRequestId": "$context.extendedRequestId",
                            "authorizerError": "$context.authorizer.error",
                            "errorMessage": "$context.error.message",
                        }
                    )
                ),
            ),
            endpoint_types=[apigw.EndpointType.REGIONAL],
        )

        Tags.of(rest_api).add(RESOURCE_NAME_TAG_PREFIX, rest_api_name)

        # Grant API Gateway permission to write logs to CloudWatch
        access_log_group = logs.LogGroup.from_log_group_name(
            self,
            "ImportedApiGatewayAccessLogs",
            log_group_name=f"/aws/apigateway/{name_prefix}-callback-api-access-logs",
        )
        access_log_group.grant_write(iam.ServicePrincipal("apigateway.amazonaws.com"))

        # Add tag to the deployed stage
        # This excepts the API from the auto applied WAF
        if config.is_slot:
            Tags.of(rest_api.deployment_stage).add("rcp_waf_cloudflare_protected", "false")

        # Submit call integration
        submit_call_integration = apigw.LambdaIntegration(
            submit_call_lambda.function,
            proxy=True,
            integration_responses=[
                apigw.IntegrationResponse(
                    status_code="200",
                    response_templates={"application/json": ""},
                )
            ],
        )

        submit_call_resource = rest_api.root.add_resource("submit-call")
        submit_call_method = submit_call_resource.add_method(
            "POST",
            submit_call_integration,
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={"application/json": apigw.Model.EMPTY_MODEL},
                )
            ],
            api_key_required=True,
        )

        submit_call_lambda.function.add_permission(
            "ApiGatewayInvokePermission",
            principal=iam.ServicePrincipal("apigateway.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=f"arn:aws:execute-api:{self.region}:{self.account}:{rest_api.rest_api_id}/*/POST/submit-call",
        )

        # Basic "health" integration
        health_resource = rest_api.root.add_resource("health")
        health_integration = apigw.MockIntegration(
            integration_responses=[
                apigw.IntegrationResponse(
                    status_code="200",
                    response_templates={"application/json": '"OK"'},
                )
            ],
            request_templates={"application/json": '{"statusCode": 200}'},
        )
        health_resource.add_method(
            "GET",
            health_integration,
            method_responses=[
                apigw.MethodResponse(
                    status_code="200",
                    response_models={"application/json": apigw.Model.EMPTY_MODEL},
                )
            ],
        )

        if not config.is_slot:
            api_key = apigw.ApiKey(
                self,
                "SubmitCallApiKey",
                api_key_name=f"{name_prefix}-submit-call-api-key",
                description="API Key for submit-call endpoint",
                enabled=True,
            )

            usage_plan = apigw.UsagePlan(
                self,
                "SubmitCallUsagePlan",
                name=f"{name_prefix}-submit-call-usage-plan",
                description="Usage plan for submit-call endpoint",
                throttle=apigw.ThrottleSettings(
                    rate_limit=10,
                    burst_limit=20,
                ),
                quota=apigw.QuotaSettings(
                    limit=10000,
                    period=apigw.Period.DAY,
                ),
            )
            usage_plan.add_api_key(api_key)

            usage_plan.add_api_stage(
                stage=rest_api.deployment_stage,
                api=rest_api,
                throttle=[
                    apigw.ThrottlingPerMethod(
                        method=submit_call_method,
                        throttle=apigw.ThrottleSettings(
                            rate_limit=10,
                            burst_limit=20,
                        ),
                    )
                ],
            )

            # Require API Key on the /submit-call POST method
            submit_call_method.add_method_response(
                status_code="403",
                response_models={"application/json": apigw.Model.EMPTY_MODEL},
            )

            # Create API mapping
            apigw.CfnBasePathMapping(
                self,
                "ApiMapping",
                domain_name=config.environment_config.callback_api_domain,
                rest_api_id=rest_api.rest_api_id,
            )

        return rest_api
