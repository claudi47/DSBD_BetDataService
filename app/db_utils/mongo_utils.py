import motor.motor_asyncio

import app.settings as config


class Mongo:
    def __init__(self):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(
            f'mongodb://{config.settings.db_username}:{config.settings.db_password}@{config.settings.db_host}:{config.settings.db_port}/')
        self._db = self.client[config.settings.db_database]

    @property
    def db(self):
        return self._db

    @property
    def client(self):
        return self._client


mongo = Mongo()
