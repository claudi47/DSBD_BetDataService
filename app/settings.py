from pydantic import BaseSettings


class Settings(BaseSettings):
    db_host: str
    db_database: str


class BrokerSettings(BaseSettings):
    broker: str
    schema_registry: str


db_settings = Settings()
broker_settings = BrokerSettings()
