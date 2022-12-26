import datetime
import json
import os
import random
import time
import logging

from kafka import KafkaProducer, admin

topic = 'stock'


log_key = 'producer'
logger = logging.getLogger(log_key)

bootstrap_server = os.getenv('BOOTSTRAP_SERVER') if os.getenv('BOOTSTRAP_SERVER') else 'localhost:9092'
logger.error("Kafka IP: " + bootstrap_server)


def serializer(obj):
    return json.dumps(obj).encode('utf-8')


try:

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=serializer
    )

    try:
        admin_client = admin.KafkaAdminClient(
            bootstrap_servers=bootstrap_server,
            client_id='kafka-python-producer-1',
        )

        topic_list = [admin.NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
    except Exception as e:
        logger.error("Can't create topic")

except Exception:
    logger.error("Can't connect to kafka")

if __name__ == '__main__':
    while True:
        data = {
            "symbol": "AMZN",
            "price": random.random(),
            "time": datetime.datetime.now().strftime("%Y/%m/%d, %H:%M:%S"),
        }
        logger.error("Send: " + str(data))
        producer.send(topic, data)
        time.sleep(random.randint(1, 11))
