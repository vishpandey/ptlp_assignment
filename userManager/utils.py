import pymongo
import requests
import uuid

from kafka import KafkaConsumer
from config import Config
from kafka_producer import Producer
from pymongo import InsertOne
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId


def getPropValue(prop):
    return Config.get(prop)

def getKafkaProducer(topic):
    return Producer(getPropValue("KAFKA_SERVERS"), getPropValue(topic))


def getKafkaConsumer(consumer_topic):
    return KafkaConsumer(getPropValue(consumer_topic),
                        group_id=getPropValue("KAFKA_CONSUMER_GROUP"),
                        bootstrap_servers=getPropValue("KAFKA_SERVERS"),
                        auto_offset_reset="latest")


def getMongoConnectionObject(db, collection):
    connection_url = getPropValue("MONGO_DB_URI")
    client = pymongo.MongoClient(connection_url)
    database_name = getPropValue(db)
    collection_name = getPropValue(collection)

    conn_obj = client[database_name][collection_name]

    return conn_obj

def extractAndStoreUserData(data):
    add_users_request = []
    for user in data["users-list"]:
        user["user-id"] = str(uuid.uuid4())[:8]

        user["series-unlocked"] = []

        db_conn_obj = getMongoConnectionObject("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")
        series_content = db_conn_obj.find()

        for series in series_content:
            series_metadata_for_user = {}
            series_metadata_for_user["series-id"] = series["series"]["series-id"]
            series_metadata_for_user["total-chapters"] = len(series["chapters"])
            series_metadata_for_user["unlocked-chapters"] = 4
            if len(series["chapters"]) < 4:
                series_metadata_for_user["unlocked-chapters"] = len(series["chapters"])    
            user["series-unlocked"].append(series_metadata_for_user)

        add_users_request.append(InsertOne(user))

    db_conn_obj = getMongoConnectionObject("MONGO_USER_DB", "MONGO_USER_COLLECTION")
    try:
        output = db_conn_obj.bulk_write(add_users_request)
    except BulkWriteError as bwe:
        print(bwe.details)



