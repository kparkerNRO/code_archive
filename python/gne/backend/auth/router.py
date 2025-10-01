from fastapi import APIRouter, Depends, Request

from agent_companion.api.auth.models import AuthenticatedResponse
from agent_companion.api.depends import (
    agent_companion_oauth_client,
    auth_secret,
    authenticated_response,
)

router = APIRouter(prefix="/auth", tags=["auth"])


@router.get("")
async def auth_callback(
    request: Request,
    auth_response: AuthenticatedResponse = Depends(authenticated_response),
) -> AuthenticatedResponse:
    return auth_response


@router.get("/login")
async def login(
    request: Request,
    callback_url_override: str | None = None,
    auth_client=Depends(agent_companion_oauth_client),
    auth_secret=Depends(auth_secret),
):
    return await auth_client.authorize_redirect(request, callback_url_override or auth_secret.callback_url)
