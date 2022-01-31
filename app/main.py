from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

import app.db_utils.mongo_utils as database
from app.db_utils import advanced_scheduler
from app.kafka import consumers, producers
from app.models import SearchDataPartialInDb

app = FastAPI()

Instrumentator().instrument(app).expose(app)

@app.on_event("startup")
def run_consumers_producers_and_scheduler():
    advanced_scheduler.init_scheduler()
    consumers.initialize_consumers()
    producers.initialize_producers()


@app.on_event("shutdown")
def close_consumers_producers():
    consumers.close_consumers()
    producers.close_producers()

@app.get("/searches/")
async def search_list():
    researches_gb = await database.mongo.db[SearchDataPartialInDb.collection_name].count_documents(
        {'web_site': 'goldbet'})
    researches_bw = await database.mongo.db[SearchDataPartialInDb.collection_name].count_documents({'web_site': 'bwin'})
    return {'goldbet': researches_gb, 'bwin': researches_bw, 'total': researches_gb + researches_bw}


