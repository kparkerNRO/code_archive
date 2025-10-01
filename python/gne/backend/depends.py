import json
import os

import boto3
import jwt
from authlib.integrations.starlette_client import OAuth
from common.database.connect import get_engine_from_secret
from common.database.models import CallCenterSsoGroupMap, CallCenterUser
from fastapi import Depends, Request
from fastapi.security.oauth2 import OAuth2AuthorizationCodeBearer
from jwt import PyJWKClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from agent_companion.api.auth.models import (
    AgentCompanionUserInfo,
    AuthenticatedResponse,
    OAuthSecret,
    OAuthSettings,
)
from agent_companion.errors import CallCenterUserNotFoundException


def boto3_session():
    return boto3.session.Session()


def db_session(boto3_session=Depends(boto3_session)):
    return Session(bind=get_engine_from_secret(os.getenv("RDS_SECRET_NAME"), boto3_session))


def oauth_settings():
    return OAuthSettings()


def secretsmanager_client():
    return boto3.client("secretsmanager")


async def auth_secret(
    oauth_settings: OAuthSettings = Depends(oauth_settings),
    secretsmanager_client=Depends(secretsmanager_client),
):
    response = secretsmanager_client.get_secret_value(SecretId=oauth_settings.secret_arn)
    secret_dict = json.loads(response["SecretString"])
    return OAuthSecret(**secret_dict)


async def oauth(
    oauth_secret: OAuthSecret = Depends(auth_secret, use_cache=True),
):
    oauth = OAuth()
    oauth.register(
        "agent-companion",
        client_id=oauth_secret.client_id,
        client_secret=oauth_secret.client_secret.get_secret_value(),
        client_kwargs={"scope": oauth_secret.scope},
        server_metadata_url=oauth_secret.server_metadata_url,
    )
    return oauth


async def agent_companion_oauth_client(oauth: OAuth = Depends(oauth, use_cache=True)):
    return oauth.create_client("agent-companion")


async def authenticated_response(
    request: Request,
    auth_client: OAuth = Depends(agent_companion_oauth_client),
):
    token = await auth_client.authorize_access_token(request)
    return AuthenticatedResponse(**token)


async def user_info(
    access_token: str = Depends(
        # The tokenUrl and authorizationUrl don't need to be set for our use
        # case. We're leveraging this to only extract the Bearer token from a
        # request and raise a 401 if it's an invalid jwt.
        OAuth2AuthorizationCodeBearer(
            tokenUrl="na",
            authorizationUrl="na",
        )
    ),
    auth_secret: OAuthSecret = Depends(auth_secret),
):
    jwks_client = PyJWKClient(auth_secret.token_endpoint)
    signing_key = jwks_client.get_signing_key_from_jwt(access_token)
    access_token_header = jwt.get_unverified_header(access_token)
    return AgentCompanionUserInfo(
        **jwt.decode(
            access_token,
            signing_key.key,
            algorithms=[access_token_header.get("alg", "RS256")],
        )
    )


async def user_id(
    user_info: AgentCompanionUserInfo = Depends(user_info),
):
    return user_info.user_id


async def user(
    user_info: AgentCompanionUserInfo = Depends(user_info),
    db_session: Session = Depends(db_session),
) -> AgentCompanionUserInfo:
    """A comprehensive user info object that optionally includes the user's call center user info if exists.

    Args:
        user_info (AgentCompanionUserInfo): The user info object from the auth server.
        db_session (Session): The database session.

    Returns:
        AgentCompanionUserInfo: The user's information with the call center user info if exists.
    """
    user_db_record = db_session.scalars(
        select(CallCenterUser).where(CallCenterUser.email == user_info.email)
    ).one_or_none()
    if user_db_record:
        user_info.call_center_user_table_id = user_db_record.id
        user_info.external_id = user_db_record.external_id
        user_info.display_name = user_db_record.display_name
        user_info.call_center_id = user_db_record.call_center_id
        user_info.active = user_db_record.active

    # Try to identify call center by using SSO group name
    if not user_info.call_center_id:
        user_info.call_center_id = db_session.scalars(
            select(CallCenterSsoGroupMap.call_center_id).where(
                CallCenterSsoGroupMap.sso_group_name.in_(user_info.roles)
            )
        ).first()

    return user_info


async def call_center_user(user_info: AgentCompanionUserInfo = Depends(user)) -> AgentCompanionUserInfo:
    """Adds a requirement to the user info object to have a call center user table id.

    Use this dependency for any endpoint that requires a call center user.

    Args:
        user_info (AgentCompanionUserInfo): The user info object from the auth server.

    Raises:
        UserNotFoundException: The user wasn't found in the call center user table.

    Returns:
        AgentCompanionUserInfo: The user info object guaranteed to include the user's call center user info.
    """
    if user_info.call_center_user_table_id is None:
        raise CallCenterUserNotFoundException("CallCenterUser not found")
    return user_info
