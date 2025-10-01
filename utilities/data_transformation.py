from typing import TypedDict

from pymongo import UpdateOne


class IDData(TypedDict):
    _id: str


def wrap_with_update_one_operation(data: IDData):
    return UpdateOne({'_id': data['_id']}, {'$set': data}, upsert=True)
