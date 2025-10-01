import json
import os
import time
from unittest.mock import patch

import boto3
import pytest
from common.database.connect import PostgresSettings
from common.database.database_fixtures import (
    CallCenterSetupFactory,
    CallFactory,
    drop_database,
    init_engine,
    test_session,
)
from common.database.models import Base
from moto import mock_aws
from pydantic import SecretStr
from s3pathlib import context as s3_context
from sqlalchemy_utils import create_database, database_exists

from data_pipeline.shared.talkdesk import TalkdeskSettings


########################################
# SQS Config
########################################
class SQSEventBridgeListener:
    """
    Test class to hold an SQS queue to listen on Event Bridge events
    and store the events for validation
    """

    def __init__(self, sqs_client, queue_name="test-events-queue"):
        # create the SQS Queue
        queue_response = sqs_client.create_queue(QueueName=queue_name)
        queue_url = queue_response["QueueUrl"]
        queue_attrs = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        self.queue_arn = queue_attrs["Attributes"]["QueueArn"]

        self.queue_url = queue_url
        self.sqs_client = sqs_client
        self.events = []

    def register_rule(self, test_events_client, event_bus_name, event_pattern):
        rule_name = "test_rule"
        test_events_client.put_rule(
            Name=rule_name, EventPattern=json.dumps(event_pattern), State="ENABLED", EventBusName=event_bus_name
        )

        test_events_client.put_targets(
            Rule=rule_name, EventBusName=event_bus_name, Targets=[{"Id": "1", "Arn": self.queue_arn}]
        )

    def poll(self, wait_time=1, buffer_time=0.5, timeout=3):
        time.sleep(buffer_time)  # Brief wait for event propagation

        start_time = time.time()
        messages_collected = 0

        while time.time() - start_time < timeout:
            # Calculate remaining messages to collect

            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=min(wait_time, int(timeout - (time.time() - start_time))),
                MessageAttributeNames=["All"],
            )
            messages = response.get("Messages", [])

            if not messages:
                # No more messages available, break early
                break

            for msg in messages:
                self.events.append(msg)
                self.sqs_client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"])
                messages_collected += 1

        return self.events

    def get_events(self):
        return self.events


########################################
# DB Config
########################################
@pytest.fixture()
def pg_settings(request):
    PG_USER = os.getenv("PG_USER", "aria")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "aria")
    PG_HOST = os.getenv("PG_HOST", "localhost")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_TEST_DB = os.getenv("PG_TEST_DB", "aria-psql-test")

    pgsettings = PostgresSettings(
        user=PG_USER,
        password=SecretStr(PG_PASSWORD),
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_TEST_DB,
    )

    yield pgsettings


@pytest.fixture()
def base_engine(pg_settings):
    engine = init_engine(pg_settings)
    try:
        if not database_exists(engine.url):
            create_database(engine.url)
        Base.metadata.create_all(engine)
        yield engine
    finally:
        if engine:
            drop_database(engine)
            engine.dispose()


@pytest.fixture()
def base_session(base_engine):
    test_session.configure(bind=base_engine)
    try:
        session = test_session()
        yield session
    finally:
        if session:
            session.rollback()
            session.close()
            test_session.remove()


########################################
# AWS Config
########################################


@pytest.fixture
def aws_env():
    with mock_aws():
        with patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}):
            yield


@pytest.fixture
def boto3_session(aws_env):
    boto3_session = boto3.session.Session()
    s3_context.attach_boto_session(boto3_session)
    yield boto3_session


@pytest.fixture
def secrets_client(pg_settings, boto3_session):
    client = boto3_session.client("secretsmanager")

    yield client


@pytest.fixture
def base_env(secrets_client):
    """
    Set up the basic environment variables that the Settings object expects.

    This MUST be first in the fixtures list, as test-specific fixtures will
    erride the patches
    """
    encryption_secret_name = "super-secret-encryption-key"
    secrets_client.create_secret(
        Name=encryption_secret_name,
        SecretString=json.dumps({"key": "00000000000000000000000000000000"}),
    )

    with patch.dict(
        os.environ,
        {
            "EVENT_IDENTIFIER": "data-pipeline.fake",
            "EVENT_BUS_NAME": "fake-bus",
            "PIPELINE_BUCKET_NAME": "fake-bucket",
            "RDS_SECRET_NAME": "fake_secret",
            "ENCRYPTION_KEY_SECRET_NAME": encryption_secret_name,
        },
    ):
        yield


@pytest.fixture
def events_client(boto3_session):
    client = boto3_session.client("events")
    yield client


@pytest.fixture
def sqs_client(boto3_session):
    client = boto3_session.client("sqs")
    yield client


@pytest.fixture()
def s3_client(boto3_session):
    s3client = boto3_session.client("s3")
    yield s3client


@pytest.fixture
def test_event_bus():
    return "test-lambda"


@pytest.fixture
def test_events_client(test_event_bus, events_client):
    with patch.dict(
        os.environ,
        {"EVENT_BUS_NAME": test_event_bus},
    ):
        events_client.create_event_bus(Name=test_event_bus)
        yield events_client


@pytest.fixture
def sqs_listener(sqs_client):
    yield SQSEventBridgeListener(sqs_client)


@pytest.fixture()
def db_secret(secrets_client, pg_settings):
    secret_value = {
        "username": pg_settings.user,
        "password": pg_settings.password.get_secret_value(),
        "host": pg_settings.host,
        "port": pg_settings.port,
        "database": pg_settings.dbname,
    }

    secret_name = "test-postgres-credentials"
    secrets_client.create_secret(Name="test-postgres-credentials", SecretString=json.dumps(secret_value))

    with patch.dict(os.environ, {"RDS_SECRET_NAME": secret_name}):
        yield secrets_client

    secrets_client.delete_secret(SecretId=secret_name)


@pytest.fixture()
def encryption_secret(secrets_client):
    encryption_secret_value = {
        "key": "ZK4XR8L1Rz4CFCGX5Dy9dZTjsG0MbO6W",
    }

    secret_name = "test-encryption-key"
    secrets_client.create_secret(Name=secret_name, SecretString=json.dumps(encryption_secret_value))

    with patch.dict(os.environ, {"ENCRYPTION_KEY_SECRET_NAME": secret_name}):
        yield secrets_client

    secrets_client.delete_secret(SecretId=secret_name)


########################################
# Fixture configuration
########################################
@pytest.fixture()
def call_center_setup(base_session, s3_client):
    return CallCenterSetupFactory.create_with_s3_setup(
        s3_client=s3_client,
    )


@pytest.fixture
def call(base_session, call_center_setup):
    call = CallFactory(call_center=call_center_setup.call_center)
    return call


########################################
# Talkdesk configuration
########################################
@pytest.fixture()
def talkdesk_secret(secrets_client):
    secret_value = {
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "token_url": "https://test.talkdeskapp.com/oauth/token",
    }

    secret_name = "test-talkdesk-credentials"
    secrets_client.create_secret(Name=secret_name, SecretString=json.dumps(secret_value))

    secret_arn = f"arn:aws:secretsmanager:us-west-2:123456789012:secret:{secret_name}"
    with patch.dict(os.environ, {"TALKDESK_SECRET_ARN": secret_arn}):
        yield secrets_client

    secrets_client.delete_secret(SecretId=secret_name)


@pytest.fixture()
def talkdesk_settings():
    """Create a TalkdeskSettings instance for testing."""
    talkdesk_settings = TalkdeskSettings(
        event_identifier="data-pipeline.talkdesk-download",
        event_bus_name="test-lambda",
        rds_secret_name="test-postgres-credentials",
        talkdesk_api_url="https://test.talkdeskapp.com/api/v2",
        talkdesk_secret_arn="arn:aws:secretsmanager:us-west-2:123456789012:secret:talkdesk-test-secret",
    )

    return talkdesk_settings


@pytest.fixture()
def talkdesk_lambda_env(base_env, talkdesk_settings: TalkdeskSettings):
    """Set up environment variables for talkdesk download lambda."""
    env_vars = {
        "EVENT_IDENTIFIER": talkdesk_settings.event_identifier,
        "TALKDESK_SECRET_ARN": talkdesk_settings.secret_arn,
        "TALKDESK_API_URL": talkdesk_settings.api_url,
    }

    with patch.dict(os.environ, env_vars):
        yield
