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

@app.get("/stats")
# le = 'less equal' - ge = 'greater equal' (between 1 and 4 included)
async def calculating_stats(stat: int = Query(..., le=4, ge=1)):
    match stat:
        case 1:
            users = await db['web_server_user'].find(projection={'_id': False, 'username': True}).to_list(None)
            return {'users': users, 'count': len(users)}
        case 2:
            return await db['web_server_search'].count_documents({})
        case 3:
            researches_gb = await db['web_server_search'].count_documents({'web_site': 'goldbet'})
            researches_bw = await db['web_server_search'].count_documents({'web_site': 'bwin'})
            return {'goldbet': researches_gb, 'bwin': researches_bw}
        case 4:
            researches_count = await db['web_server_search'].count_documents({})
            users_count = await db['web_server_user'].count_documents({})
            if users_count == 0:
                return 'empty'
            users = await db['web_server_user'].aggregate([
                {
                    '$lookup': {
                        'from': 'web_server_search',
                        'localField': 'user_identifier',
                        'foreignField': 'user_id',
                        'as': 'search'
                    }
                },
                {
                    '$project': {
                        '_id': False,
                        'username': True,
                        'count': {'$size': '$search'}
                    }
                }
            ]).to_list(None)

            return {'average': researches_count / users_count, 'users': users}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}
