import pymongo
from bson import ObjectId
from pymongo.results import InsertOneResult


class MongoDb:
    def __init__(self, connection_string="mongodb://localhost:27017/"):
        self.connection_string = connection_string

    def get_databases(self) -> list[str]:
        client = pymongo.MongoClient(self.connection_string)
        return client.list_database_names()

    def database_exist(self, database_name: str) -> bool:
        if database_name in self.get_databases():
            return True
        return False

    def get_collections(self, database_name: str) -> list[str]:
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        return db.list_collection_names()

    def collection_exist(self, database_name, collection_name: str) -> bool:
        if collection_name in self.get_collections(database_name):
            return True
        return False

    def insert_one(self, database_name: str, collection_name: str, value: dict) -> int:
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        return col.insert_one(value).inserted_id

    def insert_many(self, database_name: str, collection_name: str, list_of_value: list[dict]) -> list[int]:
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        return col.insert_many(list_of_value).inserted_ids

    def find_one(self, database_name: str, collection_name: str, query_params: dict = None,
                 field_params: dict = None) -> dict:
        """
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :param field_params:
        Example: {"_id": 0}
        :param sort_params:
        Example:
        :return:
        """
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        return col.find_one(query_params, field_params)

    def find_all(self, database_name: str, collection_name: str, query_params: dict = None, field_params: dict = None,
                 sort_params: list = None, limit_param: int = None) -> list:
        """
        :param limit_param:
        :param sort_params:
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :param field_params:
        Example: {"_id": 0}
        :param sort_params:
        Example: [('name', -1), ("_id", 1)]
        :param limit_param:
        Example: 5
        :return:
        """
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        result = col.find(query_params, field_params)
        if sort_params is not None:
            for i in range(len(sort_params)):
                result.sort(sort_params[i][0], sort_params[i][1])
        if limit_param is not None:
            result.limit(limit_param)
        return [x for x in result]

    def delete_one(self, database_name: str, collection_name: str, query_params: dict = None):
        """
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :return:
        """
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.delete_one(query_params)

    def delete_many(self, database_name: str, collection_name: str, query_params: dict = None):
        """
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :return:
        """
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.delete_many(query_params)

    def delete_all(self, database_name: str, collection_name: str):
        """
        :param database_name:
        :param collection_name:
        :return:
        """
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.delete_many({})

    def delete_database(self, database_name: str):
        client = pymongo.MongoClient(self.connection_string)
        client.drop_database(database_name)

    def delete_collection(self, database_name: str, collection_name: str):
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.drop()

    def update_one(self, database_name: str, collection_name: str, query_params: dict = None, new_values: dict = {}):
        """
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :param new_values:
        Example: { "name": "Minnie" }
        :return:
        """
        new_values = {"$set": new_values}
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.update_one(query_params, new_values)

    def update_all(self, database_name: str, collection_name: str, query_params: dict = None, new_values: dict = {}):
        """
        :param database_name:
        :param collection_name:
        :param query_params:
        Example: { "address": "Park Lane 38" } or { "address": { "$gt": "S" } } or { "address": { "$regex": "^S" } }
        :param new_values:
        Example: { "name": "Minnie" }
        :return:
        """
        new_values = {"$set": new_values}
        client = pymongo.MongoClient(self.connection_string)
        db = client[database_name]
        col = db[collection_name]
        col.update_many(query_params, new_values)


