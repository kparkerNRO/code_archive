import pytest

from agent_companion.api.auth.models import AuthenticatedResponse


@pytest.mark.asyncio
async def test_login_redirects(unauthenticated_client):
    resp = unauthenticated_client.get("/auth/login")
    assert "wamqa.roche.com" in resp.headers["location"]
    assert resp.status_code == 302


@pytest.mark.asyncio
async def test_login_redirects_when_authed(authenticated_client):
    """
    Validate that we redirect even when authorized - this is expected behavior
    """
    resp = authenticated_client.get("/auth/login")
    assert "wamqa.roche.com" in resp.headers["location"]
    assert resp.status_code == 302


@pytest.mark.asyncio
async def test_auth_callback_returns_token(unauthenticated_client):
    resp = unauthenticated_client.get("/auth?code=abc")
    assert resp.status_code == 200
    assert AuthenticatedResponse.model_validate(resp.json())
