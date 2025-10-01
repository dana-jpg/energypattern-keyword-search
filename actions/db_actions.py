import asyncio

from servicess.MongoDBConnection import MongoDBConnection


def upsert_collection(database: str, collection: str):
    mongo_conn = MongoDBConnection()
    client = mongo_conn.get_client()
    db = client[database]
    collection = db[collection]
    collection.update_one({"_id": "111"}, {"$set": {"name": "test2"}}, upsert=True)
    print(db.list_collection_names())
    print(list(collection.find()))
    return collection


async def upsert_collection_async(database: str, collection: str, data):
    try:
        mongo_conn = MongoDBConnection()
        client = mongo_conn.get_client()
        db = client[database]
        c = db[collection]
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: c.bulk_write(data)
        )
        await asyncio.sleep(5)
    except Exception as e:
        print("Error in upsert_collection_async", e)
        return None
