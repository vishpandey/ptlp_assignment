import pymongo

class DbManager:
    def __init__(self, connection_string, db, collection):
        self.url = "mongodb://" + connection_string
        self.db = db
        self.collection = collection
        self.client = pymongo.MongoClient(self.url)
        self.connObj = self.client[self.db][self.collection]


    def parseCursor(self, cursor):
        result = {}
        result["records"] = []
        for doc in cursor:
            result["records"].append(doc)

        return result

    def findOne(self, query_string):
    	cursor = self.connObj.find_one(query_string)
    	return cursor

    def findAll(self, query_string):
        cursor = self.connObj.find(query_string, {"_id": False})
        result = self.parseCursor(cursor)
        return result

    def insertOne(self, record):
    	self.connObj.insert_one(record)

    def update(self, query_string, update_query):
    	self.connObj.update_one(query_string, update_query)

    def aggregate(self, query_string):
        self.connObj.aggregate(query_string)
