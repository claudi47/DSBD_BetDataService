import asyncio
import motor.motor_asyncio


async def get_doc():
    client = motor.motor_asyncio.AsyncIOMotorClient(
        'mongodb://claudi47:ciaoatutti@localhost:27018/'
    )
    db = client['db_dsbd']
    doc = await db['search_history'].insert_many({'alfa': i} for i in range(1, 4))
    print('ciao')

asyncio.run(get_doc())