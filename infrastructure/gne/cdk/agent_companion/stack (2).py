from aws_cdk import (
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    Tags,
    aws_certificatemanager as acm,
    aws_cloudfront as cloudfront,
    aws_cloudfront_origins as origins,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_iam as iam,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct
from settings import RESOURCE_NAME_TAG_PREFIX, HopperConfig


class AgentCompanionServiceStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config: HopperConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        name_prefix_for_imports = config.name_prefix_import
        name_prefix_short = config.name_prefix_short

        vpc = config.environment_config.vpc(self)
        public_subnet_selection = config.environment_config.public_subnet_selection(self)
        private_subnet_selection = config.environment_config.private_subnet_selection(self)

        ecs_cluster_name = Fn.import_value(f"{name_prefix_for_imports}-EcsClusterName")
        pipeline_bucket_arn = Fn.import_value(f"{name_prefix_for_imports}-PipelineBucketArn")
        ecs_cluster = ecs.Cluster.from_cluster_attributes(
            self,
            "EcsCluster",
            cluster_name=ecs_cluster_name,
            vpc=vpc,
        )

        rds_security_group_id = Fn.import_value(f"{name_prefix_for_imports}-RDSSecurityGroupId")
        rds_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "RdsSecurityGroup",
            security_group_id=rds_security_group_id,
        )

        ##############################
        # Application Load Balancer
        ##############################

        alb = self._init_alb(
            config,
            vpc,
            public_subnet_selection,
            rds_security_group,
        )

        listener = self._init_alb_listener(
            alb, name_prefix_short, config.environment_config.backend_certificate_arn, vpc
        )

        ##############################
        # ECS Task Definition
        ##############################
        pipeline_bucket = s3.Bucket.from_bucket_arn(self, "PipelineBucket", pipeline_bucket_arn)
        self._init_ecs(
            config, vpc, ecs_cluster, private_subnet_selection, listener, rds_security_group, pipeline_bucket
        )

        ##############################
        # Agent Companion Frontend
        ##############################

        distribution = self._init_frontend(config)

        ##############################
        # Outputs
        ##############################
        CfnOutput(self, "ALBEndpoint", value=alb.load_balancer_dns_name)
        CfnOutput(self, "DistributionDNS", value=distribution.domain_name)

    def _init_alb(
        self,
        config: HopperConfig,
        vpc,
        public_subnet_selection,
        rds_security_group,
    ):
        name_prefix = config.name_prefix
        name_prefix_for_imports = config.name_prefix_import
        name_prefix_short = config.name_prefix_short
        # Purpose: Allow traffic from cloudflare to ALB
        #
        # Be sure to also follow the instructions listed here.
        #
        # https://sites.google.com/roche.com/rcp/aws/security-boundaries/securely-exposing-api-gw-in-rcp?authuser=0
        #
        # They have an integration with CloudFlare that needs to be
        # setup correctly in order to bypass a WAF rule.

        alb_security_group = ec2.SecurityGroup(
            self,
            "AlbSecurityGroup",
            vpc=vpc,
            description="Security group for Application Load Balancer",
            security_group_name=f"{name_prefix}-alb-sg",
            allow_all_outbound=True,
        )
        alb_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(80),
            "Allow inbound HTTP traffic on port 80",
        )
        alb_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(443),
            "Allow inbound HTTPs traffic on 443",
        )

        rds_security_group.add_ingress_rule(
            alb_security_group,
            ec2.Port.tcp(5432),
            "Allow PostgreSQL access from ECS tasks",
        )

        alb_name = f"{name_prefix_short}-backend-alb"
        alb = elbv2.ApplicationLoadBalancer(
            self,
            "ApplicationLoadBalancer",
            vpc=vpc,
            internet_facing=True,
            security_group=alb_security_group,
            load_balancer_name=alb_name,
            vpc_subnets=public_subnet_selection,
            deletion_protection=True,
            cross_zone_enabled=True,
            http2_enabled=True,
            desync_mitigation_mode=elbv2.DesyncMitigationMode.DEFENSIVE,
            idle_timeout=Duration.seconds(60),
            client_keep_alive=Duration.seconds(3600),
            drop_invalid_header_fields=False,
            preserve_host_header=False,
            x_amzn_tls_version_and_cipher_suite_headers=False,
            preserve_xff_client_port=False,
            xff_header_processing_mode=elbv2.XffHeaderProcessingMode.APPEND,
            waf_fail_open=False,
        )

        Tags.of(alb).add(RESOURCE_NAME_TAG_PREFIX, alb_name)

        CfnOutput(
            self,
            "AlbArn",
            value=alb.load_balancer_arn,
            export_name=f"{name_prefix_for_imports}-AlbArn",
        )

        CfnOutput(
            self,
            "AlbSecGroupId",
            value=alb_security_group.security_group_id,
            export_name=f"{name_prefix_for_imports}-AlbSecGroupId",
        )

        return alb

    def _init_alb_listener(self, alb, name_prefix_short, certificate_arn, vpc):
        certificate = acm.Certificate.from_certificate_arn(self, "ApiCert", certificate_arn)
        listener = alb.add_listener(
            "Listener",
            port=443,
            certificates=[certificate],
            ssl_policy=elbv2.SslPolicy.TLS13_RES,
            open=True,
        )

        return listener

    def _init_ecs(
        self,
        config: HopperConfig,
        vpc,
        ecs_cluster,
        private_subnet_selection,
        listener,
        rds_security_group,
        pipeline_bucket,
    ):
        name_prefix_for_imports = config.name_prefix_import
        name_prefix = config.name_prefix
        backend_repository_uri = Fn.import_value(f"{name_prefix_for_imports}-BackendRepositoryUri")

        rds_secret_name = Fn.import_value(f"{name_prefix_for_imports}-DbCredentialsSecretName")
        encryption_key_secret_name = Fn.import_value(f"{name_prefix_for_imports}-DataEncryptionKeySecretName")

        deployment_role = iam.Role(
            self,
            "AEDPipelineDeploymentRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
            role_name=f"{name_prefix}-backend-deployment-role",
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
                resources=[f"arn:aws:ecr:{self.region}:{self.account}:repository/{backend_repository_uri}"],
            )
        )

        task_role = iam.Role(
            self,
            "AEDPipelineTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ],
            role_name=f"{name_prefix}-backend-task-role",
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
                actions=["secretsmanager:GetSecretValue"],
                resources=[config.environment_config.oauth_secret_arn],
            )
        )

        task_role.add_to_policy(
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
                    "kms:ListKeys",
                ],
                resources=["*"],
            )
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{rds_secret_name}*"],
            ),
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["secretsmanager:GetSecretValue"],
                resources=[f"arn:aws:secretsmanager:{self.region}:{self.account}:secret:{encryption_key_secret_name}*"],
            ),
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[pipeline_bucket.bucket_arn + "/*"],
            ),
        )

        task_definition = ecs.FargateTaskDefinition(
            self,
            "TaskDefinition",
            cpu=8192,  # 8 vCPU
            memory_limit_mib=16384,  # 16GB
            family=f"{name_prefix}-backend-task-def",
            execution_role=deployment_role,
            task_role=task_role,
        )

        log_group = logs.LogGroup(
            self,
            "AEServiceLogGroup",
            log_group_name=f"/ecs/{name_prefix}-ae-service",
            retention=logs.RetentionDays.FIVE_MONTHS,
            removal_policy=RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        )

        log_group.add_metric_filter(
            "AEServiceErrorMetricFilter",
            filter_pattern=logs.FilterPattern.any_term("error", "ERROR", "Error", "500"),
            metric_namespace="AriaService",
            metric_name=f"{name_prefix}-ae-service-errors",
            default_value=0,
            metric_value="1",
        )

        task_definition.add_container(
            "WebContainer",
            image=ecs.ContainerImage.from_registry(f"{backend_repository_uri}:{config.docker_tag}"),
            logging=ecs.LogDriver.aws_logs(
                stream_prefix=f"{name_prefix}-ae-service",
                log_group=log_group,
            ),
            port_mappings=[
                ecs.PortMapping(
                    container_port=5602,
                    protocol=ecs.Protocol.TCP,
                    name="backend-agent-companion",
                ),
            ],
            environment={
                "OAUTH_SECRET_ARN": config.environment_config.oauth_secret_arn,
                "RDS_SECRET_NAME": rds_secret_name,
                "ENCRYPTION_KEY_SECRET_NAME": encryption_key_secret_name,
            },
        )

        # Allow inbound traffic from ALB to ECS tasks
        ecs_security_group = ec2.SecurityGroup(
            self,
            "EcsSecurityGroup",
            vpc=vpc,
            description="Security group for ECS tasks",
            security_group_name=f"{name_prefix}-ecs-sg",
        )

        ecs_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(5602),
            "Allow access to containers from within the VPC",
        )

        rds_security_group.add_ingress_rule(
            ecs_security_group, ec2.Port.tcp(5432), "Allow PostgreSQL access from ECS tasks"
        )

        for key, value in config.resource_tags.items():
            Tags.of(ecs_security_group).add(key, value)

        ae_detection_service_name = f"{name_prefix}-backend-ae-service"
        ae_detection_service = ecs.FargateService(
            self,
            "AEDetectionService",
            cluster=ecs_cluster,
            task_definition=task_definition,
            desired_count=1,
            vpc_subnets=private_subnet_selection,
            assign_public_ip=False,
            security_groups=[ecs_security_group],
            service_name=ae_detection_service_name,
            circuit_breaker=ecs.DeploymentCircuitBreaker(
                enable=True,
                rollback=True,
            ),
        )

        Tags.of(ae_detection_service).add(RESOURCE_NAME_TAG_PREFIX, ae_detection_service_name)

        listener.add_targets(
            "EcsTargets",
            port=80,
            targets=[ae_detection_service],
            health_check=elbv2.HealthCheck(
                path="/health",
                interval=Duration.seconds(30),
                timeout=Duration.seconds(5),
                healthy_http_codes="200",
            ),
        )

    def _init_frontend(self, config):
        # Purpose: Allow traffic from cloudflare to cloudfront.
        #
        # Be sure to also follow the instructions listed here.
        #
        # https://sites.google.com/roche.com/rcp/aws/security-boundaries/securely-exposing-information-using-s3-buckets
        #
        # They have an integration with CloudFlare that needs to be
        # setup correctly in order to bypass a WAF rule.
        site_bucket_name = f"{config.name_prefix}-static-assets"
        site_bucket = s3.Bucket(
            self,
            "SiteBucket",
            bucket_name=site_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        Tags.of(site_bucket).add(RESOURCE_NAME_TAG_PREFIX, site_bucket_name)

        # Purpose: Authenticate cert w/ cloudflare
        #
        # IT is responsible for generating ACM certs. Once they are
        # generated you'll need to create CNAME records for cloudflare
        # validation purposes. Follow the instructions below.
        #
        # https://sites.google.com/roche.com/rcp/aws/security-boundaries/securely-exposing-web-applications-in-rcp
        certificate = acm.Certificate.from_certificate_arn(self, "SiteCert", config.frontend_cert_arn)

        distribution = cloudfront.Distribution(
            self,
            "SiteDistribution",
            enable_logging=True,
            default_behavior=cloudfront.BehaviorOptions(
                origin=origins.S3BucketOrigin.with_origin_access_control(
                    site_bucket,
                    origin_access_levels=[
                        cloudfront.AccessLevel.READ,
                        cloudfront.AccessLevel.LIST,
                    ],
                ),
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
            ),
            domain_names=[config.frontend_domain_name],
            certificate=certificate,
            default_root_object="index.html",
            error_responses=[
                cloudfront.ErrorResponse(http_status=404, response_http_status=200, response_page_path="/index.html")
            ],
        )

        # This policy was in place while the distribution was observed
        # working. However, it is redundant while using the
        # `with_origin_access_control` on the distribution above.
        # Keeping for now until Cloudflare is configured.
        site_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject"],
                resources=[site_bucket.arn_for_objects("*")],
                principals=[iam.ServicePrincipal("cloudfront.amazonaws.com")],
                conditions={"StringEquals": {"AWS:SourceArn": distribution.distribution_arn}},
            )
        )

        return distribution
