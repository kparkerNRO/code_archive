import json

import boto3
from pydantic_settings import BaseSettings, SettingsConfigDict

_cached_key = None


class EncryptionSettings(BaseSettings):
    encryption_key_secret_name: str

    model_config = SettingsConfigDict(env_prefix="")


def _get_encryption_key(settings: EncryptionSettings, boto3_session: boto3.session.Session) -> bytes:
    """
    Get the KMS-encrypted AES key from Secrets Manager, decrypt it with KMS,
    and return it as a 32-byte key for encryption libraries.
    """
    global _cached_key

    if _cached_key:
        return _cached_key

    secrets_client = boto3_session.client("secretsmanager")

    response = secrets_client.get_secret_value(SecretId=settings.encryption_key_secret_name)
    key_str = json.loads(response["SecretString"])["key"]

    # Convert the ASCII string to 32-byte AES key
    key_bytes = key_str.encode("utf-8")

    _cached_key = key_bytes

    if len(key_bytes) != 32:
        raise ValueError("Key length is not 32 bytes")

    return key_bytes


def get_encryption_key(settings: EncryptionSettings | None = None):
    """
    Get the KMS-encrypted AES key from Secrets Manager with default settings, decrypt it with KMS,
    and return it as a 32-byte key for encryption libraries.
    """
    settings = settings or EncryptionSettings()
    boto3_session = boto3.session.Session()

    return _get_encryption_key(settings, boto3_session)
