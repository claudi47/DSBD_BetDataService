import os

from apscheduler.jobstores.mongodb import MongoDBJobStore

job_stores = {
    'default': MongoDBJobStore(host=f'mongodb://{os.getenv("DB_USERNAME")}:{os.getenv("DB_PASSWORD")}'
                                    f'@{os.getenv("DB_URL")}:{os.getenv("DB_PORT")}/')
}