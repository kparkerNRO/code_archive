import json
import os
from unittest.mock import patch

import boto3
import factory
import pytest
from common.database.connect import PostgresSettings
from common.database.database_fixtures import (
    CallCenterSetupFactory,
    drop_database,
    init_engine,
    test_session,
)
from common.database.models import (
    Base,
)
from fastapi.testclient import TestClient
from moto import mock_aws
from pydantic import SecretStr
from s3pathlib import context as s3_context
from sqlalchemy_utils import create_database, database_exists

from agent_companion.api.auth.model_factories import AgentCompanionUserInfoFactory
from agent_companion.api.auth.models import AuthenticatedResponse, OAuthSettings
from agent_companion.api.depends import (
    authenticated_response,
    boto3_session,
    db_session,
    secretsmanager_client,
    user_info,
)
from agent_companion.main import app

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


@pytest.fixture()
def boto_session(aws_env):
    boto3_session = boto3.session.Session()
    s3_context.attach_boto_session(boto3_session)
    yield boto3_session


@pytest.fixture
def oauth_secret_json():
    return json.dumps(
        {
            "client_id": "aria_dev",
            "token_endpoint": "https://wamqa.roche.com/ext/JWKSaria",
            "sso_url": "https://wamqa.roche.com/as/authorization.oauth2?client_id=aria_dev&scope=openid+profile&response_type=code&redirect_uri=https://aria-dev.roche.com/auth",
            "auth_endpoint": "https://wamqa.roche.com/as/token.oauth2",
            "client_secret": "fakesecret",
            "callback url": "https://aria-dev.roche.com/auth",
        }
    )


@pytest.fixture
def secretsmanager(aws_env, oauth_secret_json):
    secretsmanager = boto3.client("secretsmanager", region_name="us-west-2")
    secretsmanager.create_secret(Name="test-secret", SecretString=oauth_secret_json)
    os.environ["OAUTH_SECRET_ARN"] = "test-secret"
    yield secretsmanager
    del os.environ["OAUTH_SECRET_ARN"]


@pytest.fixture()
def db_secret(secretsmanager, pg_settings):
    secret_value = {
        "username": pg_settings.user,
        "password": pg_settings.password.get_secret_value(),
        "host": pg_settings.host,
        "port": pg_settings.port,
        "database": pg_settings.dbname,
    }

    secret_name = "test-postgres-credentials"
    secretsmanager.create_secret(Name="test-postgres-credentials", SecretString=json.dumps(secret_value))

    with patch.dict(os.environ, {"RDS_SECRET_NAME": secret_name}):
        yield secretsmanager

    secretsmanager.delete_secret(SecretId=secret_name)


@pytest.fixture()
def encryption_secret(secretsmanager):
    encryption_secret_value = {
        "key": "ZK4XR8L1Rz4CFCGX5Dy9dZTjsG0MbO6W",
    }

    secret_name = "test-encryption-key"
    secretsmanager.create_secret(Name=secret_name, SecretString=json.dumps(encryption_secret_value))

    with patch.dict(os.environ, {"ENCRYPTION_KEY_SECRET_NAME": secret_name}):
        yield secretsmanager

    secretsmanager.delete_secret(SecretId=secret_name)


########################################
# Fixture configuration
########################################


@pytest.fixture()
def call_center_setup(base_session):
    return CallCenterSetupFactory.create()


@pytest.fixture
def call_center(call_center_setup):
    return call_center_setup.call_center


@pytest.fixture
def agent_companion_user_info(call_center_setup):
    return AgentCompanionUserInfoFactory.from_call_center_user(call_center_setup.agent)


@pytest.fixture
def agent_companion_supervisor_user_info(call_center_setup):
    return AgentCompanionUserInfoFactory.from_call_center_user(call_center_setup.supervisor)


########################################
# Auth Config
########################################


def _get_oauth_response(user_info: dict):
    return {
        "access_token": "eyJhbGc...",
        "refresh_token": "HyHIywMw73...",
        "id_token": "eyJhbGciOiJSUz...",
        "token_type": "Bearer",
        "expires_in": 7199,
        "expires_at": 1748980389,
        "userinfo": user_info,
    }


@pytest.fixture
def oauth_response(agent_companion_user_info):
    return _get_oauth_response(agent_companion_user_info.model_dump())


@pytest.fixture
def authenticated_user(agent_companion_user_info):
    """Create authenticated user with database record using factory."""
    return agent_companion_user_info


@pytest.fixture
def authenticated_supervisor(base_session, call_center_setup):
    return AgentCompanionUserInfoFactory.from_call_center_supervisor(call_center_setup.supervisor)


########################################
# Test Client Config
########################################


def _create_authenticated_app(authenticated_user, secretsmanager, base_session, boto_session):
    def user_info_override():
        return authenticated_user

    def db_session_override():
        yield base_session

    def boto3_session_override():
        yield boto_session

    app.dependency_overrides[db_session] = db_session_override
    app.dependency_overrides[boto3_session] = boto3_session_override
    app.dependency_overrides[user_info] = user_info_override
    app.dependency_overrides[secretsmanager_client] = lambda: secretsmanager
    app.dependency_overrides[OAuthSettings] = lambda: OAuthSettings(secret_arn="test-secret")

    return app


@pytest.fixture
def authenticated_app(authenticated_user, secretsmanager, base_session, boto_session):
    return _create_authenticated_app(authenticated_user, secretsmanager, base_session, boto_session)


@pytest.fixture
def authenticated_app_supervisor(authenticated_supervisor, secretsmanager, base_session, boto_session):
    return _create_authenticated_app(authenticated_supervisor, secretsmanager, base_session, boto_session)


@pytest.fixture
def unauthenticated_app(authenticated_app, call_center_setup):
    oauth_response = _get_oauth_response(factory.build(dict, FACTORY_CLASS=AgentCompanionUserInfoFactory))

    def authenticated_response_override():
        return AuthenticatedResponse.model_validate(oauth_response)

    del app.dependency_overrides[user_info]
    app.dependency_overrides[authenticated_response] = authenticated_response_override
    yield app
    app.dependency_overrides = {}


@pytest.fixture
def unauthenticated_client(unauthenticated_app):
    return TestClient(unauthenticated_app, follow_redirects=False)


@pytest.fixture
def authenticated_client(authenticated_app):
    return TestClient(authenticated_app, follow_redirects=False)


@pytest.fixture
def authenticated_client_supervisor(authenticated_app_supervisor):
    return TestClient(authenticated_app_supervisor, follow_redirects=False)


client = authenticated_client  # Alias for less typing
client_supervisor = authenticated_client_supervisor
