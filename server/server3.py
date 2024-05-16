"""
This script runs the final part of the model inference
This save the inference output to the database and
communicate with the dispatcher node via kafka
This server also takes in user input and gives back final music recommendation


"""
# Import Required Modules
import json
import numpy as np
import logging
from tensorflow.keras.models import load_model
from mongo_db.mongo_db import MongoDB
from kakfa_handler.kafka_handler import KafkaHandler
from model.recomendation import get_reccomendations

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creating stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

def run_training_part_3():

    mongo_cli = MongoDB()
    # Load the partial RNN model
    model_part3 = load_model('RnnModel/model_part3.h5')

    if not mongo_cli.file_in_gridfs("output_part3.txt"):
        data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part2.txt')
        input_data = np.loadtxt(data_buffer)
        # Run the partial inference anf get the required final results
        predictions = model_part3.predict(input_data)
        np.savetxt('output_part3.txt', predictions)
        # Saving this partial result in mongoDB GRIDFS
        mongo_cli.add_file_to_gridfs(file_path='output_part3.txt')

    data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part3.txt')
    input_data = np.loadtxt(data_buffer)
    return input_data


def process_kafka_messages():
    prediction = run_training_part_3()
    # Initialize Kafka Producer and Consumer Queues
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["partial-inference-3"]) # Subscribe to the consumer topic
    while True:
        message = consumer.poll(3000)
        if message is None:
            continue
        for topic, messages in message.items():
            for message in messages:
                logger.info("Received final-recommendation message")
                logger.info(message.value)
                track_id_list = message.value.split(",")
                # from the final model inference getting the user recommendation
                # based on user songs selection
                results = get_reccomendations(predictions=prediction, track_id_list=track_id_list)
                logger.info("final results: ", results)
                # Sending the final recommended songs back to dispatcher node
                producer.send("recommendation", value=results)
                logger.info("Sent recommendation message")
    consumer.close()  # Closing the consumer


# Run the function
process_kafka_messages()
