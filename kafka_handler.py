from kafka import KafkaConsumer
from kafka import KafkaProducer
import json


class KafkaHandler:
    def __init__(self, key=None, kafka_producer_topic=None, kafka_producer_value=None, func_name=None):
        self.KAFKA_BROKER_URL = 'localhost:9092'

        # self.key = key
        # self.kafka_producer_topic = kafka_producer_topic
        # self.func_name = func_name
        # self.message_store = []

    def initialize_kafka_consumer(self):
        # Kafka consumer configuration

        # Create a Kafka consumer
        consumer = KafkaConsumer(
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )

        return consumer

        # # Process messages from Kafka
        # try:
        #     for message in consumer:
        #         message_val = message.value
        #         if message_val is not None:
        #             for each in message_val:
        #                 if self.key in each:
        #                     value = each[self.key]
        #                     self.message_store.append(value)
        #                 else:
        #                     print(f"Message does not contain the key {self.key}")
        #         else:
        #             print("Received an empty or invalid message")
        #         self.kafkaProducer(value=self.kafka_producer_value, topic_name=self.kafka_producer_topic)
        #         # consumer.subscribe(topics=self.kafka_topic, on_message=)
        #
        # except json.JSONDecodeError as e:
        #     print("Failed to decode JSON:", e)

    def initialize_kafka_producer(self):

        # Create a Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
        )

        return producer

        # try:
        #     producer.send(self.kafka_topic, value=value)
        #     print(f"Succesfully sent message to {topic_name}")
        # except json.JSONDecodeError as e:
        #     print("Failed to decode JSON:", e)
        # finally:
        #     producer.flush()
        #     producer.close()



