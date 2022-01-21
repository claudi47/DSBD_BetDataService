from fastapi import FastAPI

from app.db_utils import advanced_scheduler
from app.kafka import consumers

app = FastAPI()


@app.on_event("startup")
def run_consumers_and_scheduler():
    advanced_scheduler.init_scheduler()
    consumers.initialize_consumers()

@app.on_event("shutdown")
def close_consumers():
    consumers.close_consumers()

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
