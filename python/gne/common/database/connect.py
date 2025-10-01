import json
from urllib.parse import quote_plus

from pydantic import SecretStr
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine


class PostgresSettings(BaseSettings):
    user: str = "hopper"
    password: SecretStr = SecretStr("hopper")
    host: str = "localhost"
    dbname: str = "postgres"
    port: str = "5432"

    class Config:
        env_prefix = "pg_"

    @property
    def url(self) -> str:
        return "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
            self.user,
            quote_plus(self.password.get_secret_value()),
            self.host,
            self.port,
            self.dbname,
        )

    @property
    def url_no_pwd(self) -> str:
        return "postgresql://%s@%s:%s/%s" % (
            self.user,
            self.host,
            self.port,
            self.dbname,
        )


def get_engine(settings: PostgresSettings | None = None):
    settings = settings or PostgresSettings()
    return create_engine(url=settings.url)


def get_engine_from_secret(secret_name, boto3_session):
    secret_client = boto3_session.client("secretsmanager")
    secret = secret_client.get_secret_value(SecretId=secret_name)["SecretString"]
    secret = json.loads(secret)

    settings = PostgresSettings(
        user=secret["username"],
        password=SecretStr(secret["password"]),
        host=secret["host"],
        dbname=secret.get("database", "postgres"),
        port=str(secret["port"]),
    )
    return get_engine(settings)
