import json
import logging
import os

from kafka import KafkaConsumer

log_key = 'producer'
logger = logging.getLogger(log_key)

topic = 'stock'

bootstrap_server = (os.getenv('BOOTSTRAP_SERVER_IP') if os.getenv('BOOTSTRAP_SERVER_IP') else 'localhost') + ':9092'
logger.error("Kafka IP: " + bootstrap_server)

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_server,
    auto_offset_reset='earliest'
)

if __name__ == '__main__':
    for message in consumer:
        bytes_value = message.value
        json_str = bytes_value.decode('utf8').replace("'", '"')
        obj = json.loads(json_str)
        logger.error("Receive: " + json_str)
