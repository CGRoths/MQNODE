from __future__ import annotations

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'MQNODE'
    app_env: str = 'dev'
    log_level: str = 'INFO'

    postgres_host: str = Field('postgres', alias='POSTGRES_HOST')
    postgres_port: int = Field(5432, alias='POSTGRES_PORT')
    postgres_db: str = Field('mqnode', alias='POSTGRES_DB')
    postgres_user: str = Field('mqnode', alias='POSTGRES_USER')
    postgres_password: str = Field('mqnode', alias='POSTGRES_PASSWORD')

    redis_host: str = Field('redis', alias='REDIS_HOST')
    redis_port: int = Field(6379, alias='REDIS_PORT')

    btc_rpc_host: str = Field('bitcoin', alias='BTC_RPC_HOST')
    btc_rpc_port: int = Field(8332, alias='BTC_RPC_PORT')
    btc_rpc_user: str = Field('bitcoin', alias='BTC_RPC_USER')
    btc_rpc_password: str = Field('bitcoin', alias='BTC_RPC_PASSWORD')
    btc_rpc_timeout: int = Field(30, alias='BTC_RPC_TIMEOUT')

    btc_listener_sleep_seconds: int = Field(10, alias='BTC_LISTENER_SLEEP_SECONDS')

    @property
    def postgres_dsn(self) -> str:
        return (
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password} host={self.postgres_host} port={self.postgres_port}"
        )

    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/0"

    @property
    def btc_rpc_url(self) -> str:
        return f"http://{self.btc_rpc_host}:{self.btc_rpc_port}"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
