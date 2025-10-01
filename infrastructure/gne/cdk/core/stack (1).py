from aws_cdk import (
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    Tags,
    aws_apigateway as apigw,
    aws_certificatemanager as acm,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_events as events,
    aws_iam as iam,
    aws_kms as kms,
    aws_rds as rds,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_servicediscovery as servicediscovery,
)
from constructs import Construct
from settings import RESOURCE_NAME_TAG_PREFIX, HopperConfig


class HopperCoreStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, config: HopperConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        name_prefix = config.name_prefix
        vpc = config.environment_config.vpc(self)
        private_subnet_selection = config.environment_config.private_subnet_selection(self)

        ##############################
        # Security Groups
        ##############################
        rds_security_group = ec2.SecurityGroup(
            self,
            "RdsSecurityGroup",
            vpc=vpc,
            description="Security group for RDS PostgreSQL cluster",
            security_group_name=f"{name_prefix}-rds-sg",
        )

        # Allow PostgreSQL traffic  RDS
        rds_security_group.add_ingress_rule(
            ec2.Peer.any_ipv4(),
            ec2.Port.tcp(5432),
            "Allow PostgreSQL access from within the VPC",
        )

        ##############################
        # ECR Repositories
        ##############################
        backend_repo_name = f"{name_prefix}-backend"
        backend_repo = ecr.Repository(
            self,
            "BackendRepo",
            repository_name=backend_repo_name,
            removal_policy=RemovalPolicy.RETAIN,
        )

        Tags.of(backend_repo).add(RESOURCE_NAME_TAG_PREFIX, backend_repo_name)

        ae_detection_repo_name = f"{name_prefix}-pipeline"
        ae_detection_repo = ecr.Repository(
            self,
            "PipelineRepo",
            repository_name=ae_detection_repo_name,
            removal_policy=RemovalPolicy.RETAIN,
        )

        Tags.of(ae_detection_repo).add(RESOURCE_NAME_TAG_PREFIX, ae_detection_repo_name)

        ##############################
        # ECS Cluster
        ##############################
        cluster_name = f"{name_prefix}-cluster"
        cluster = ecs.Cluster(
            self,
            "EcsCluster",
            cluster_name=cluster_name,
            vpc=vpc,
            container_insights_v2=ecs.ContainerInsights.ENABLED,
            default_cloud_map_namespace=ecs.CloudMapNamespaceOptions(
                name=f"{name_prefix}.local",
                type=servicediscovery.NamespaceType.DNS_PRIVATE,
                vpc=vpc,
            ),
        )

        Tags.of(cluster).add(RESOURCE_NAME_TAG_PREFIX, cluster_name)

        ##############################
        # RDS PostgreSQL Cluster
        ##############################
        # Create credentials
        kms_key = kms.Key(
            self,
            "DatabaseEncryptionKey",
            alias=f"alias/{name_prefix}-db-key",
            description=f"KMS key for {name_prefix} database encryption",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        kms_data_key = kms.Key(
            self,
            "DataEncryptionKey",
            alias=f"alias/{name_prefix}-data-encryption-key",
            description=f"KMS key for {name_prefix} PII data encryption",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        db_credentials_secret = secretsmanager.Secret(
            self,
            "DatabaseCredentialsSecret",
            secret_name=f"{name_prefix}-db-credentials",
            description=f"Secret for {name_prefix} RDS PostgreSQL credentials",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "postgres"}',
                generate_string_key="password",
                exclude_punctuation=True,
                exclude_characters='"@/\\',
                password_length=16,
            ),
            encryption_key=kms_key,
        )

        data_encryption_key_secret = secretsmanager.Secret(
            self,
            "DataEncryptionKeySecret",
            secret_name=f"{name_prefix}-data-encryption-key",
            description=f"Secret for {name_prefix} data encryption key",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template="{}",
                generate_string_key="key",
                password_length=32,
            ),
            encryption_key=kms_data_key,
        )

        # Create the cluster
        db_cluster_name = f"{name_prefix}-postgres"
        db_cluster = rds.DatabaseCluster(
            self,
            "PostgresCluster",
            cluster_identifier=db_cluster_name,
            engine=rds.DatabaseClusterEngine.aurora_postgres(version=rds.AuroraPostgresEngineVersion.VER_15_4),
            credentials=rds.Credentials.from_secret(db_credentials_secret),
            vpc=vpc,
            vpc_subnets=private_subnet_selection,  # Use explicit subnet selection
            instance_identifier_base=f"{name_prefix}-postgres-instance",
            writer=rds.ClusterInstance.serverless_v2(
                "writer",
                scale_with_writer=False,
            ),
            readers=[
                rds.ClusterInstance.serverless_v2(
                    "reader",
                    scale_with_writer=True,
                ),
            ],
            serverless_v2_min_capacity=0.5,  # Minimum ACU (Aurora Capacity Units)
            serverless_v2_max_capacity=16,  # Maximum ACU
            port=5432,
            security_groups=[rds_security_group],
            storage_encrypted=True,
            storage_encryption_key=kms_key,
            backup=rds.BackupProps(
                retention=Duration.days(7),
                preferred_window="03:00-04:00",  # 3-4am UTC
            ),
            preferred_maintenance_window="sat:05:00-sat:06:00",  # Saturday 5-6am UTC
            removal_policy=RemovalPolicy.SNAPSHOT,
        )

        Tags.of(db_cluster).add(RESOURCE_NAME_TAG_PREFIX, db_cluster_name)

        ##############################
        # Event Bridge
        ##############################
        event_bus_name = f"{name_prefix}-event-bus"
        event_bus = events.EventBus(
            self,
            "EventBus",
            event_bus_name=event_bus_name,
        )

        Tags.of(event_bus).add(RESOURCE_NAME_TAG_PREFIX, event_bus_name)

        ##############################
        # Bastion Host
        ##############################

        if config.environment_config.bastion_ami:
            bastion_security_group = ec2.SecurityGroup(
                self,
                "BastionSecurityGroup",
                vpc=vpc,
                description="Security group for the bastion host",
                security_group_name=f"{name_prefix}-bastion-sg",
            )

            az_config = config.environment_config.vpc_availability_zones[0]
            bastion_host_name = f"{name_prefix}-bastion"
            bastion_host = ec2.BastionHostLinux(
                self,
                "BastionHost",
                instance_type=ec2.InstanceType("t3.medium"),
                vpc=vpc,
                instance_name=bastion_host_name,
                availability_zone=az_config.az_name,
                machine_image=ec2.MachineImage.generic_linux(
                    {
                        config.environment_config.region: config.environment_config.bastion_ami,
                    }
                ),
                subnet_selection=ec2.SubnetSelection(
                    subnets=[
                        ec2.Subnet.from_subnet_attributes(
                            self,
                            "TestSubnet",
                            subnet_id=az_config.private_subnet_ids[0],
                            availability_zone=az_config.az_name,
                        )
                    ],
                ),
                security_group=bastion_security_group,
            )
            bastion_host.role.add_to_principal_policy(
                iam.PolicyStatement(
                    actions=[
                        "rds:Describe*",
                        "rds:ListTagsForResource",
                    ],
                    resources=["*"],
                )
            )
            bastion_host.role.add_to_principal_policy(
                iam.PolicyStatement(
                    actions=[
                        "secretsmanager:GetSecretValue",
                        "secretsmanager:DescribeSecret",
                    ],
                    resources=[db_credentials_secret.secret_arn],
                )
            )
            bastion_host.role.add_to_principal_policy(
                iam.PolicyStatement(
                    actions=[
                        "kms:Decrypt",
                    ],
                    resources=[kms_key.key_arn],
                )
            )

            Tags.of(bastion_host).add(RESOURCE_NAME_TAG_PREFIX, bastion_host_name)

        ##############################
        # API Configuration
        ##############################
        # Create IAM role for API Gateway CloudWatch logging
        # This role allows API Gateway to push access logs and execution logs to CloudWatch
        api_gateway_cloudwatch_role = iam.Role(
            self,
            "ApiGatewayCloudWatchRole",
            assumed_by=iam.ServicePrincipal("apigateway.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonAPIGatewayPushToCloudWatchLogs")
            ],
            role_name=f"{name_prefix}-api-gateway-cloudwatch-role",
        )

        # Configure API Gateway account to use the CloudWatch role
        # This sets the CloudWatch role ARN at the account level for API Gateway
        apigw.CfnAccount(
            self,
            "ApiGatewayAccount",
            cloud_watch_role_arn=api_gateway_cloudwatch_role.role_arn,
        )

        # Create a custom domain for API Gateway
        api_gateway = apigw.DomainName(
            self,
            "ApiCustomDomain",
            domain_name=config.environment_config.callback_api_domain,
            certificate=acm.Certificate.from_certificate_arn(
                self,
                "ApiDomainCert",
                config.environment_config.callback_api_certificate_arn,
            ),
            endpoint_type=apigw.EndpointType.REGIONAL,
            security_policy=apigw.SecurityPolicy.TLS_1_2,
        )
        api_gateway.apply_removal_policy(RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE)

        ##############################
        # Pipeline bucket(s)
        ##############################

        # TODO eventually this will be one-per-call-center
        # with naming scheme: {name_prefix}-{call_center_name}
        pipeline_bucket = self._init_call_center(name_prefix, config.environment_config.call_center_tag)

        ##############################
        # Outputs
        ##############################
        CfnOutput(
            self,
            "ApiGWDomainRecord",
            value=api_gateway.domain_name_alias_domain_name,
            export_name=f"{name_prefix}-ApiGWDomainRecord",
        )

        CfnOutput(
            self,
            "DbCredentialsSecretName",
            value=db_credentials_secret.secret_name,
            export_name=f"{name_prefix}-DbCredentialsSecretName",
        )
        CfnOutput(
            self,
            "DataEncryptionKeySecretName",
            value=data_encryption_key_secret.secret_name,
            export_name=f"{name_prefix}-DataEncryptionKeySecretName",
        )
        CfnOutput(
            self,
            "DbClusterName",
            value=db_cluster.cluster_endpoint.hostname,
            export_name=f"{name_prefix}-DbClusterHostname",
        )
        CfnOutput(
            self,
            "EventBusArn",
            value=event_bus.event_bus_arn,
            export_name=f"{name_prefix}-EventBusArn",
        )
        CfnOutput(
            self,
            "EcsClusterName",
            value=cluster.cluster_arn,
            export_name=f"{name_prefix}-EcsClusterName",
        )
        CfnOutput(
            self,
            "BackendRepositoryUri",
            value=backend_repo.repository_uri,
            export_name=f"{name_prefix}-BackendRepositoryUri",
        )
        CfnOutput(
            self,
            "PipelineRepositoryName",
            value=ae_detection_repo.repository_name,
            export_name=f"{name_prefix}-PipelineRepositoryName",
        )
        CfnOutput(
            self,
            "RDSSecurityGroupId",
            value=rds_security_group.security_group_id,
            export_name=f"{name_prefix}-RDSSecurityGroupId",
        )

        CfnOutput(
            self,
            "ClusterNamespace",
            value=cluster.default_cloud_map_namespace.namespace_name,
            export_name=f"{name_prefix}-ClusterNamespace",
        )

        CfnOutput(
            self,
            "ClusterNamespaceName",
            value=cluster.default_cloud_map_namespace.namespace_name,
            export_name=f"{name_prefix}-ClusterNamespaceName",
        )

        CfnOutput(
            self,
            "ClusterNamespaceArn",
            value=cluster.default_cloud_map_namespace.namespace_arn,
            export_name=f"{name_prefix}-ClusterNamespaceArn",
        )

        CfnOutput(
            self,
            "ClusterNamespaceId",
            value=cluster.default_cloud_map_namespace.namespace_id,
            export_name=f"{name_prefix}-ClusterNamespaceId",
        )

        if config.environment_config.bastion_ami:
            CfnOutput(
                self,
                "BastionHostInstanceId",
                value=bastion_host.instance_id,
                export_name=f"{name_prefix}-BastionHostInstanceId",
            )

        CfnOutput(
            self,
            "CfnAPIRoleArn",
            value=api_gateway_cloudwatch_role.role_arn,
            export_name=f"{name_prefix}-CfnAPIRoleArn",
        )

        CfnOutput(
            self,
            "PipelineBucketArn",
            value=pipeline_bucket.bucket_arn,
            description="ARN for the pipeline bucket",
            export_name=f"{name_prefix}-PipelineBucketArn",
        )

    def _init_call_center(self, name_prefix, call_ctr_tag):
        pipeline_bucket_name = f"{name_prefix}-{call_ctr_tag}"
        pipeline_bucket = s3.Bucket(
            self,
            "PipelineBucket",
            bucket_name=pipeline_bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(id="ExpireAfter90Days", expiration=Duration.days(90)),
                s3.LifecycleRule(id="AudioExpiresAfter7Days", expiration=Duration.days(7), prefix="calls/"),
                s3.LifecycleRule(
                    id="TranscriptsExpireAfter180Days", expiration=Duration.days(180), prefix="transcripts/"
                ),
            ],
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )

        Tags.of(pipeline_bucket).add(RESOURCE_NAME_TAG_PREFIX, pipeline_bucket_name)

        return pipeline_bucket
