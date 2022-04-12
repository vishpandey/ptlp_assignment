import yaml
import threading
import json
import time

from utils import *

db_conn_obj = getMongoConnectionObject("MONGO_CONTENT_DB", "MONGO_CONTENT_COLLECTION")
content_upload_consumer = getKafkaConsumer("BULK_CONTENT_UPLOAD_TOPIC")
# add_content_to_user_producer = getKafkaProducer("KAFKA_UPDATE_SERIES_IN_USERS_TOPIC")

for msg in content_upload_consumer:
    print(msg.value)

    upload_content_message = json.loads(msg.value.decode('utf-8'))
    inserted_series_meta = extractAndStoreSeriesData(upload_content_message) 

    produce_payload_for_user_updation = {"new-series-metadata":inserted_series_meta}
    getKafkaProducer("KAFKA_UPDATE_SERIES_IN_USERS_TOPIC").produce(produce_payload_for_user_updation)

    time.sleep(1)