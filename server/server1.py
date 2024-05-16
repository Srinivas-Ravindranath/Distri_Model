"""
This script runs the initial part of the model inference
This save the inference output to the database and
communicate with the dispatcher node via kafka


"""
# Import Required Modules
import numpy as np
from tensorflow.keras.models import load_model
import json
import logging

from kakfa_handler.kafka_handler import KafkaHandler
from mongo_db.mongo_db import MongoDB
from model.load import load_data

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creating stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

def inference_part_1():
    # Load the partial RNN model
    model_part1 = load_model('RnnModel/model_part1.h5')
    # Getting the required data from the dataset
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')
    # Run the partial inference anf get the required partial results
    output_part1 = model_part1.predict(artist_indices.reshape(-1, 1))
    concatenated_inputs = np.concatenate([output_part1, X], axis=1)
    np.savetxt('output_part1.txt', concatenated_inputs)  # Storing these results in a text file
    mongo_cli = MongoDB()
    mongo_cli.add_file_to_gridfs(file_path='output_part1.txt')  # Saving this partial result in mongoDB GRIDFS


def process_kafka_messages():
    # Initialize Kafka Producer and Consumer Queues
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["partial-inference-1"])  # Subscribe to the consumer topic
    while True:
        message = consumer.poll(3000)  # Do the polling of the kafka consumer every 3 seconds
        if message is None:
            continue
        # Iterate over the Topics and messages
        for topic, messages in message.items():
            for message in messages:
                json_value = json.loads(message.value)
                if json_value['inference_exists'] == "False":
                    logger.info("Message received")
                    logger.info("Running inference part 1")
                    inference_part_1()
                    logger.info("Inference part 1 complete")
                    producer.send("partial-inference-1", value=json.dumps({"inference_exists": "True"}))
                    logger.info("Sent partial-inference-1 message")
    consumer.close() # Closing the Kafka consumer

# This is the initial function calling
process_kafka_messages()