from typing import Dict, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

from common.database.models import CallCenterSsoGroupMap
from common.pydantic_helpers import coerce_fields
from pydantic import BaseModel, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import select

# Simple dictionary cache for supervisor status
# Using a global dictionary instead of functools.lru_cache because we need to
# use the existing database session passed to the method (not create a new one)
_supervisor_cache: Dict[Tuple[Tuple[str, ...], int], bool] = {}


def get_cached_supervisor_status(roles: list[str], call_center_id: int) -> bool | None:
    """Get cached supervisor status if available.

    Args:
        roles: List of user roles
        call_center_id: Call center ID

    Returns:
        bool | None: Cached result if available, None if not cached
    """
    cache_key = (tuple(sorted(roles)), call_center_id)
    return _supervisor_cache.get(cache_key)


def cache_supervisor_status(roles: list[str], call_center_id: int, is_supervisor: bool):
    """Cache supervisor status result.

    Args:
        roles: List of user roles
        call_center_id: Call center ID
        is_supervisor: Whether the user is a supervisor
    """
    cache_key = (tuple(sorted(roles)), call_center_id)
    _supervisor_cache[cache_key] = is_supervisor


class OAuthSettings(BaseSettings):
    secret_arn: str
    model_config = SettingsConfigDict(env_prefix="oauth_")


class OAuthSecret(BaseModel):
    client_id: str
    token_endpoint: str
    sso_url: str
    auth_endpoint: str
    client_secret: SecretStr
    callback_url: str

    @property
    def server_metadata_url(self) -> str:
        """Reuses the scheme and host to build the metadata url."""
        return urljoin(self.token_endpoint, "/.well-known/openid-configuration")

    @property
    def scope(self) -> str:
        """Extracts the scope from the sso url.

        Note: parse_qs returns this structure
        {
            "client_id": ["aria_dev"],
            "scope": ["openid profile"],
            "response_type": ["code"],
            "redirect_uri": ["https://aria-dev.roche.com/auth"],
        }
        """
        query: dict[str, list[str]] = parse_qs(urlparse(self.sso_url).query)
        return query["scope"][0]

    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data):
        """Coerce fields into our naming convention.

        Roche includes some non standard fields in their oauth responses.
        """
        mapped_columns = {
            "callback url": "callback_url",
        }
        return coerce_fields(mapped_columns, data)


class AuthModelBase(BaseModel):
    @model_validator(mode="before")
    @classmethod
    def _coerce(cls, data):
        """Coerce fields into our naming convention.

        Roche includes some non standard fields in their oauth responses.
        """
        mapped_columns = {
            "Role": "role",
            "Roles": "roles",
            "userID": "user_id",
            "useremail": "email",
            "userinfo": "user_info",
        }
        data = coerce_fields(mapped_columns, data)

        # Always return a list of roles
        if "roles" not in data and "role" in data:
            data["roles"] = data["role"]
            del data["role"]

        # In the event the role is a single string, turn it into a list
        if "roles" in data and isinstance(data["roles"], str):
            data["roles"] = [data["roles"]]

        return data


class UserInfo(AuthModelBase):
    # https://www.rfc-editor.org/rfc/rfc7523.txt
    # Mentioned in part 3: JWT Format and Processing Requirements
    iss: str  # Issuer
    iat: int  # Issued at time
    exp: int  # Expiration time


class AgentCompanionUserInfo(UserInfo):
    # Fields added by roche
    roles: list[str]
    firstname: str
    user_id: str
    lastname: str
    email: str
    # Fields from call center user table
    call_center_user_table_id: int | None = None
    external_id: str | None = None
    display_name: str | None = None
    call_center_id: int | None = None
    active: bool | None = None

    def is_supervisor(self, db_session) -> bool:
        """Check if the user is a supervisor based on CallCenterSsoGroupMap table.

        This method uses caching to avoid repeated database queries for the same
        user roles and call center combination.

        Args:
            db_session: SQLAlchemy database session

        Returns:
            bool: True if any of the user's roles are marked as supervisor in the database
        """
        if not self.roles or self.call_center_id is None:
            return False

        # Check cache first
        cached_result = get_cached_supervisor_status(self.roles, self.call_center_id)
        if cached_result is not None:
            return cached_result

        # If not cached, perform database query
        supervisor_group = db_session.scalars(
            select(CallCenterSsoGroupMap).where(
                CallCenterSsoGroupMap.sso_group_name.in_(self.roles),
                CallCenterSsoGroupMap.is_supervisor.is_(True),
                CallCenterSsoGroupMap.call_center_id == self.call_center_id,
            )
        ).first()

        result = supervisor_group is not None

        # Cache the result for future use
        cache_supervisor_status(self.roles, self.call_center_id, result)

        return result


class AuthenticatedResponse(AuthModelBase):
    access_token: str
    refresh_token: str
    id_token: str
    token_type: str
    expires_in: int
    expires_at: int
    user_info: AgentCompanionUserInfo
