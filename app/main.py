from fastapi import FastAPI

from app.db_utils.mongo_utils import mongo

app = FastAPI()


@app.get("/")
async def root():
    data = mongo.db
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
