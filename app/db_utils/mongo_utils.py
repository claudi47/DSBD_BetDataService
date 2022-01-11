import motor.motor_asyncio

from app.settings import settings


class Mongo:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            f'mongodb://{settings.db_username}:{settings.db_password}@{settings.db_host}:{settings.db_port}/')
        self.db = self.client[settings.db_database]

mongo = Mongo()
