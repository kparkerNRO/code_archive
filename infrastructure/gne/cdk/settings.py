import re
import warnings
from enum import Enum
from typing import Annotated

import aws_cdk as cdk
from constructs import Construct
from pydantic import AfterValidator, BaseModel, ValidationInfo
from pydantic.types import StringConstraints

RESOURCE_NAME_TAG_PREFIX = "CMGOasis - Name"


# validation functions
def _validate_account_region(
    value: str,
    info: ValidationInfo,
    account_regex: str,
    resource_type: str,
    region_override: str | None = None,
) -> str:
    """Generic validation function for account ID and region matching.

    Args:
        value: The value to validate (ARN or ECR repository URL)
        info: Validation info from Pydantic
        account_regex: Regex pattern with named groups 'account' and 'region'
        resource_type: Type of resource for warning messages (e.g., "ARN", "ECR repository")
        region_override: Override region to check against instead of environment region
    """
    if not info.data:
        return value

    # Extract account and region using provided regex with named groups
    match = re.search(account_regex, value)
    if not match:
        return value

    extracted_region = match.group("region")
    extracted_account = match.group("account")

    env_account = info.data.get("account")
    env_region = region_override or info.data.get("region")

    # Validate account ID
    if env_account and extracted_account != env_account:
        warnings.warn(
            f"{resource_type} in field '{info.field_name}' has account ID '{extracted_account}' "
            f"which doesn't match the environment account '{env_account}'"
        )

    # Validate region
    if env_region and extracted_region != env_region:
        warnings.warn(
            f"{resource_type} in field '{info.field_name}' has region '{extracted_region}' "
            f"which doesn't match the environment region '{env_region}'"
        )

    return value


def validate_arn_account_region(value: str, info: ValidationInfo, region_override: str | None = None) -> str:
    """Validate ARN account ID and region against environment config."""
    return _validate_account_region(
        value=value,
        info=info,
        account_regex=r"arn:aws:[^:]*:(?P<region>[^:]*):(?P<account>\d+):",
        resource_type="ARN",
        region_override=region_override,
    )


def validate_ecr_account_region(value: str, info: ValidationInfo) -> str:
    """Validate ECR repository account ID and region against environment config."""
    return _validate_account_region(
        value=value,
        info=info,
        account_regex=r"^(?P<account>\d{12})\.dkr\.ecr\.(?P<region>[a-z0-9-]+)\.amazonaws\.com/",
        resource_type="ECR repository",
        region_override=None,
    )


def validate_frontend_cert_arn(value: str, info: ValidationInfo) -> str:
    """Validate frontend certificate ARN must be in us-east-1."""
    return validate_arn_account_region(value, info, "us-east-1")


def validate_environment_mismatch(value: str, info: ValidationInfo) -> str:
    """Validate that string values don't contain wrong environment indicators.

    Checks for environment names separated by '.' or '-' in the value.
    """
    if not info.data:
        return value

    env_value = info.data.get("environment")
    if not env_value:
        return value

    if hasattr(env_value, "value"):
        env_value = env_value.value

    all_environments = {env.value for env in Environment}
    current_env = env_value.lower()

    wrong_environments = all_environments - {current_env}

    value_lower = value.lower()
    for wrong_env in wrong_environments:
        # Check for patterns like: "something-prod-something", "something.prod.something", etc.
        # Using word boundaries to avoid false positives with substrings
        import re

        pattern = rf"(?:^|[.-]){re.escape(wrong_env)}(?:[.-]|$)"
        if re.search(pattern, value_lower):
            warnings.warn(
                f"Field '{info.field_name}' contains '{wrong_env}' environment indicator "
                f"but current environment is '{current_env}'. Value: '{value}'"
            )

    return value


class Environment(str, Enum):
    DEV = "dev"
    QA = "qa"
    PROD = "prod"


class CoreConfig(BaseModel):
    department: str
    platform: str
    project: str
    tags: dict[str, str]


class VpcAz(BaseModel):
    az_name: str
    public_subnet_ids: list[str]
    private_subnet_ids: list[str]
    connection_subnet_ids: list[str]


# Validated types that check account/region matching
ValidatedArnString = Annotated[
    str,
    StringConstraints(pattern=r"^arn:aws:[a-z0-9-]+:[a-z0-9-]+:\d{12}:[a-zA-Z0-9-_/:.]+$", strict=True),
    AfterValidator(validate_arn_account_region),
]

FrontendCertArnString = Annotated[
    str,
    StringConstraints(pattern=r"^arn:aws:[a-z0-9-]+:[a-z0-9-]+:\d{12}:[a-zA-Z0-9-_/:.]+$", strict=True),
    AfterValidator(validate_frontend_cert_arn),
]

ValidatedEcrString = Annotated[
    str,
    StringConstraints(
        pattern=r"^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9_/-]+(?::[a-zA-Z0-9._-]+)?$", strict=True
    ),
    AfterValidator(validate_ecr_account_region),
]

NumericString = Annotated[str, StringConstraints(pattern=r"^\d+$", strict=True)]


# String type that validates environment consistency
EnvironmentConsistentString = Annotated[
    str,
    AfterValidator(validate_environment_mismatch),
]

# Combined validated types for fields that should check both ARN format and environment
ValidatedEnvironmentArnString = Annotated[
    str,
    StringConstraints(pattern=r"^arn:aws:[a-z0-9-]+:[a-z0-9-]+:\d{12}:[a-zA-Z0-9-_/:.]+$", strict=True),
    AfterValidator(validate_arn_account_region),
    AfterValidator(validate_environment_mismatch),
]

ValidatedEnvironmentEcrString = Annotated[
    str,
    StringConstraints(
        pattern=r"^\d{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9_/-]+(?::[a-zA-Z0-9._-]+)?$", strict=True
    ),
    AfterValidator(validate_ecr_account_region),
    AfterValidator(validate_environment_mismatch),
]


class EnvironmentConfig(BaseModel):
    environment: Environment
    account: NumericString
    region: str
    vpc_id: EnvironmentConsistentString

    vpc_availability_zones: list[VpcAz]

    ae_detection_repo_account: str
    ae_detection_region: str | None = None
    ae_detection_repo_name: str
    ae_detection_image_tag: EnvironmentConsistentString

    deepgram_secret_arn: ValidatedEnvironmentArnString

    bastion_ami: str | None = None
    talkdesk_secret_arn: ValidatedEnvironmentArnString
    talkdesk_api_url: str = "https://api.talkdeskapp.com"

    oauth_secret_arn: ValidatedEnvironmentArnString

    callback_api_domain: EnvironmentConsistentString
    callback_api_certificate_arn: ValidatedEnvironmentArnString

    # Certificates must reside in us-east-1 to be used with a cloudfront distribution
    frontend_certificate_arn: FrontendCertArnString
    frontend_domain_name: EnvironmentConsistentString

    # Backend config
    # This can be in any region
    backend_domain_name: EnvironmentConsistentString
    backend_certificate_arn: ValidatedEnvironmentArnString | None = None

    # Call center config(s)
    call_center_tag: str = "access-solutions"

    @property
    def ae_detection_repo(self):
        return (
            f"{self.ae_detection_repo_account}.dkr.ecr.{self.ae_detection_region or self.region}"
            f".amazonaws.com/{self.ae_detection_repo_name}:{self.ae_detection_image_tag}"
        )

    @property
    def ae_detection_arn(self):
        return (
            f"arn:aws:ecr:{self.ae_detection_region or self.region}:{self.ae_detection_repo_account}:"
            f"repository/{self.ae_detection_repo_name}"
        )

    @property
    def public_subnet_ids(self):
        return [subnet_id for az in self.vpc_availability_zones for subnet_id in az.public_subnet_ids]

    @property
    def private_subnet_ids(self):
        return [subnet_id for az in self.vpc_availability_zones for subnet_id in az.private_subnet_ids]

    @property
    def connection_subnet_ids(self):
        return [subnet_id for az in self.vpc_availability_zones for subnet_id in az.connection_subnet_ids]

    @property
    def vpc_availability_zone_ids(self):
        return [az.az_name for az in self.vpc_availability_zones]

    def public_subnet_selection(self, context: Construct):
        return cdk.aws_ec2.SubnetSelection(
            subnets=[
                cdk.aws_ec2.Subnet.from_subnet_id(context, f"PublicSubnet{i}", subnet_id)
                for i, subnet_id in enumerate(self.public_subnet_ids)
            ]
        )

    def private_subnet_selection(self, context: Construct):
        return cdk.aws_ec2.SubnetSelection(
            subnets=[
                cdk.aws_ec2.Subnet.from_subnet_id(context, f"PrivateSubnet{i}", subnet_id)
                for i, subnet_id in enumerate(self.private_subnet_ids)
            ]
        )

    def connection_subnet_selection(self, context: Construct):
        return cdk.aws_ec2.SubnetSelection(
            subnets=[
                cdk.aws_ec2.Subnet.from_subnet_id(context, f"ConnectionSubnet{i}", subnet_id)
                for i, subnet_id in enumerate(self.connection_subnet_ids)
            ]
        )

    def vpc(self, context: Construct) -> cdk.aws_ec2.IVpc:
        return cdk.aws_ec2.Vpc.from_vpc_attributes(
            context,
            "Vpc",
            vpc_id=self.vpc_id,
            availability_zones=self.vpc_availability_zone_ids,
            private_subnet_ids=self.private_subnet_ids,
            public_subnet_ids=self.public_subnet_ids,
        )


class HopperConfig(BaseModel):
    environment: Environment
    docker_tag: str = "latest"
    core: CoreConfig
    environment_config: EnvironmentConfig

    # dev slot support
    dev_slot: str | None = None

    @classmethod
    def from_config(cls, data, environment: Environment):
        if environment.value not in data["environments"]:
            raise ValueError(f"Environment '{environment.value}' not found in config data")

        if "core" not in data:
            raise ValueError("Core configuration not found in config data")

        return cls(
            environment=environment,
            core=CoreConfig(**data["core"]),
            docker_tag=data.get("docker_tag", "latest"),
            dev_slot=data.get("dev_slot"),
            environment_config=EnvironmentConfig(**data["environments"][environment.value]),
        )

    @property
    def is_slot(self) -> bool:
        return self.dev_slot is not None and self.environment == Environment.DEV

    @property
    def name_prefix_import(self):
        return f"{self.core.department}-{self.core.platform}-{self.environment.value}-{self.core.project}"

    @property
    def name_prefix(self):
        dev_prefix = f"{self.dev_slot}-slot-" if self.dev_slot else ""
        return f"{dev_prefix}{self.core.department}-{self.core.platform}-{self.environment.value}-{self.core.project}"

    @property
    def name_prefix_short(self):
        dev_prefix = f"{self.dev_slot}-" if self.dev_slot else ""
        return f"{dev_prefix}{self.core.department}-{self.core.platform}-{self.environment.value}"

    @property
    def resource_tags(self):
        return {
            "CMGOasis - Department": self.core.tags["department"],
            "CMGOasis - Platform": self.core.tags["platform"],
            "CMGOasis - Project": self.core.tags["project"],
            "CMGOasis - SubjectArea": self.core.tags["subject_area"],
            "CMGOasis - WBSCode": self.core.tags["wbs_code"],
            "CMGOasis - MonthlyBudget": self.core.tags["monthly_budget"],
            "CMGOasis - Environment": self.environment.value,
        }

    @property
    def frontend_domain_name(self):
        return self.environment_config.frontend_domain_name

    @property
    def frontend_cert_arn(self):
        return self.environment_config.frontend_certificate_arn

    @property
    def backend_domain_name(self):
        return self.environment_config.backend_domain_name

    @property
    def backend_cert_arn(self):
        return self.environment_config.backend_certificate_arn
