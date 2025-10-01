from common.logging import getLogger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.middleware.sessions import SessionMiddleware

from agent_companion.api.auth.router import router as auth_router
from agent_companion.api.exception_handlers import exception_handlers
from agent_companion.api.pre.router import router as pre_router
from agent_companion.api.user_activity.router import router as user_activity_router

logger = getLogger("uvicorn")


class ApiSettings(BaseSettings):
    # Required for the SessionMiddleware. Authlib stores encrypted state in cookies.
    session_secret: SecretStr = SecretStr("c3VwZXJzZWNyZXRrZXk=")
    model_config = SettingsConfigDict(env_prefix="api_")


app = FastAPI(
    title="ARIA API",
    description="API that powers ARIA",
    exception_handlers=exception_handlers,
)
app.add_middleware(
    SessionMiddleware,
    secret_key=ApiSettings().session_secret.get_secret_value(),
    same_site="none",
    https_only=True,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://aria-dev.roche.com",
        "https://aria-qa.roche.com",
        "https://aria.roche.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(auth_router)
app.include_router(pre_router)
app.include_router(user_activity_router)


@app.get("/health")
async def healthcheck():
    return "OK"
