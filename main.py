from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os
import time
from splitModel import split_model



# Function to check if all model parts exist
def check_model_parts():
    parts = ["model_part1.h5", "model_part2.h5", "model_part3.h5"]
    return all(os.path.exists("RnnModel/"+part) for part in parts)


def process_kafka_messages():
    # Kafka consumer configuration
    KAFKA_TOPIC = 'song-ids'
    KAFKA_BROKER_URL = 'localhost:9092'  # Adjust as per your Kafka broker config

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER_URL],
        auto_offset_reset='earliest',  # Start reading at the earliest message
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None  # Deserialize JSON if not empty
    )
    # List to store all track IDs
    track_ids = []

    # Process messages from Kafka
    try:
        for message in consumer:
            message_val = message.value
            if message_val is not None:
                # Iterate through each entry in the received message
                for each in message_val:
                    if 'trackId' in each:
                        track_id = each['trackId']
                        track_ids.append(track_id)
                        print(f"Track IDs: {track_id}")
                    else:
                        print("No trackids found in this message")
                kafkaProducer(','.join(track_ids))
            else:
                print("Received an empty or invalid message")

    except KeyboardInterrupt:
        print("Stopped by user.")
    except json.JSONDecodeError as e:
        print("Failed to decode JSON:", e)

    # Send processed track IDs to Kafka producer
    # kafkaProducer(track_ids)

def kafkaProducer(track_ids):
    # Kafka producer configuration
    KAFKA_TOPIC = 'server-1'
    KAFKA_BROKER_URL = 'localhost:9092'  # Adjust as per your Kafka broker config

    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
    )

    # # Convert NumPy array to list for JSON serialization
    # X_list = X.tolist()
    #
    # # Convert DataFrame to dictionary
    # df_dict = df.to_dict(orient='records')  # Converts each row into a dictionary

    # Send messages to Kafka
    try:
        message = {'trackId': track_ids}
        producer.send(KAFKA_TOPIC, value=message)
        print(f"Sent processed track IDs: {track_ids}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    print("Checking model files and connecting to Kafka...")

    # Ensure all required model files are present before starting the Kafka consumer
    while not os.path.exists("RnnModel/model.h5") or not check_model_parts():
        print("Required model files are missing. Running split_model.py to generate model parts.")
        split_model()
        print("Checking again in 30 seconds...")
        time.sleep(30)

    print("All model files are present. Connecting to Kafka...")

    process_kafka_messages()
