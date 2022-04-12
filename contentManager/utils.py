import pymongo
import time
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


def extractAndStoreSeriesData(data):
    add_series_to_user_producer = getKafkaProducer("KAFKA_UPDATE_SERIES_IN_USERS_TOPIC")
    
    requests = []
    inserted_series_metadata = []

    conn_obj = getMongoConnectionObject("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")

    for series in data["series-list"]:
        series_id = str(uuid.uuid4())[:8]
        series["series"]["series-id"] = series_id
        series["series"]["total-chapters"] = len(series["chapters"])
        requests.append(InsertOne(series))
        inserted_series_metadata.append({"series-id":series_id, "total-chapters":len(series["chapters"]), 
                                        "unlocked-chapters":4})
    try:
        output = conn_obj.bulk_write(requests)
    except BulkWriteError as bwe:
        print(bwe.details)

    
    return inserted_series_metadata