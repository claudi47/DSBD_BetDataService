from pydantic import BaseSettings


class Settings(BaseSettings):
    db_username: str
    db_password: str
    db_host: str
    db_port: int
    db_database: str

settings = Settings()
