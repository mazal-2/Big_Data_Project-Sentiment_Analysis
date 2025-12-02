from pymongo import MongoClient
from pymongo.errors import BulkWriteError

class MongoAPI(object):

    def __init__(self, db_name: str, collection_name: str, host='localhost', port=27017):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection = collection_name
        self.client = MongoClient(host=self.host, port=self.port)
        self.database = self.client[self.db_name]
        self.collection = self.database[self.collection]

    def insert_one(self, kv_dict):
        self.collection.insert_one(kv_dict)

    def insert_many(self, li_dict):
        try:
            self.collection.insert_many(li_dict, ordered=False)
        except BulkWriteError:
            pass

    def insert_many2(self, li_dict, batch_size=100):
        for i in range(0, len(li_dict), batch_size): # 字典长度并依据batch_size参数依次写入
            batch = li_dict[i:i + batch_size] #
            try:
                slef.collection.insert_many(batch,ordered=False)
            except BulkWriteError as bwe:
                print(f"⚠️ 数据库写入错误 (batch {i // batch_size}): {bwe.details}")
                pass
                # 保留失败数据用于后续重试

    def find_one(self, query1, query2):
        return self.collection.find_one(query1, query2)

    def find(self, query1, query2):
        return self.collection.find(query1, query2)

    def find_first(self):
        return self.collection.find_one(sort=[('_id', 1)])
    
    def find_last(self):
        return self.collection.find_one(sort=[('_id', -1)])

    def count_documents(self):
        return self.collection.count_documents({})

    def update_one(self, kv_dict):
        self.collection.update_one(kv_dict, {'$set': kv_dict}, upsert=True)

    def drop(self):
        self.collection.drop()
