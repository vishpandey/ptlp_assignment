import json
import time

from utils.utils import *

if __name__ == '__main__':
    content_upload_consumer = getKafkaConsumer("REQUEST_MANAGER_TOPIC")

    for msg in content_upload_consumer:

        # print(msg.value)
        
        upload_content_message = json.loads(msg.value.decode('utf-8'))

        if not validateUploadRequest(upload_content_message):
            continue
        topic = getProducerTopic(upload_content_message["isContent"])
        
        print("producing to", topic)
        getKafkaProducer(topic).produce(upload_content_message)
        
        time.sleep(1)


