import yaml
import threading
import json

from utils import getKafkaConsumer, getMongoConnectionObject, extractAndStoreUserData

db_conn_obj = getMongoConnectionObject("MONGO_USER_DB", "MONGO_USER_COLLECTION")
user_upload_consumer = getKafkaConsumer("BULK_USER_UPLOAD_TOPIC")

for msg in user_upload_consumer:
    print(msg.value)

    upload_user_message = json.loads(msg.value.decode('utf-8'))
    extractAndStoreUserData(upload_user_message)