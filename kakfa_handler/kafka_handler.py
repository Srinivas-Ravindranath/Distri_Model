from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import logging

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


class KafkaHandler:
    def __init__(self):
        self.KAFKA_BROKER_URL = 'localhost:9092'

    def initialize_kafka_consumer(self):
        # Kafka consumer configuration

        # Create a Kafka consumer
        consumer = KafkaConsumer(
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )

        return consumer

    def initialize_kafka_producer(self):
        # Create a Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON data
            acks='all',
            retries=1
        )

        return producer
