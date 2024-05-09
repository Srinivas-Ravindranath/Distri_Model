from kafka import KafkaConsumer
from kafka import KafkaProducer
import json


class KafkaHandler:
    def __init__(self, kafka_topic, key, kafka_producer_topic, kafka_producer_value, func_name):
        self.KAFKA_BROKER_URL = 'localhost:9092'
        self.kafka_producer_value = kafka_producer_value
        self.kafka_topic = kafka_topic
        self.key = key
        self.kafka_producer_topic = kafka_producer_topic
        self.func_name = func_name
        self.message_store = []

    def process_kafka_messages(self) -> str:
        # Kafka consumer configuration

        # Create a Kafka consumer
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
        )

        # Process messages from Kafka
        try:
            for message in consumer:
                message_val = message.value
                if message_val is not None:
                    for each in message_val:
                        if self.key in each:
                            value = each[self.key]
                            self.message_store.append(value)
                        else:
                            print(f"Message does not contain the key {self.key}")
                else:
                    print("Received an empty or invalid message")
                self.kafkaProducer(value=self.kafka_producer_value, topic_name=self.kafka_producer_topic)
                consumer.subscribe(topics=self.kafka_topic, on_message=)

        except json.JSONDecodeError as e:
            print("Failed to decode JSON:", e)

    def kafkaProducer(self, value: str, topic_name, ):

        # Create a Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[self.KAFKA_BROKER_URL],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
        )

        try:
            producer.send(self.kafka_topic, value=value)
            print(f"Succesfully sent message to {topic_name}")
        except json.JSONDecodeError as e:
            print("Failed to decode JSON:", e)
        finally:
            producer.flush()
            producer.close()



