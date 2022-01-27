import motor.motor_asyncio

import app.settings as config


class Mongo:
    def __init__(self):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(config.db_settings.db_host)
        self._db = self.client[config.db_settings.db_database]

    @property
    def db(self):
        return self._db

    @property
    def client(self):
        return self._client


mongo = Mongo()
