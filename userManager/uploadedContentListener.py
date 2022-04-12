import yaml
import threading
import json

from utils import getKafkaConsumer, getMongoConnectionObject

content_upload_consumer = getKafkaConsumer("KAFKA_UPDATE_SERIES_IN_USERS_TOPIC")

for msg in content_upload_consumer:
    print(msg.value)
    new_series_upload_message = json.loads(msg.value.decode('utf-8'))
    new_series_list = new_series_upload_message["new-series-metadata"]

    db_conn_obj = getMongoConnectionObject("MONGO_USER_DB", "MONGO_USER_COLLECTION")
    cursor = db_conn_obj.find()

    for doc in cursor:
        unlocked_series = doc["series-unlocked"]
        for new_series in  new_series_list:
            new_series_payload = {}
            new_series_payload["series-id"] = new_series["series-id"]
            new_series_payload["unlocked-chapters"] = new_series["unlocked-chapters"]
            new_series_payload["total-chapters"] = new_series["total-chapters"]
            if new_series["total-chapters"] < 4:
                new_series_payload["unlocked-chapters"] = new_series["total-chapters"]

            unlocked_series.append(new_series_payload)
        db_conn_obj.update_one({"_id" :doc["_id"]}, {"$set": {"series-unlocked": unlocked_series}})


    