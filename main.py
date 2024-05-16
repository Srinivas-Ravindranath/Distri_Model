"""
This is the main module
this script acts as the dispatcher node
it checks if the partial model inference are present or not in the database
and runs the server to get the partial model inference.
This gets the user selected songs and gets the final recommendation from the server3 and
publishes it back to the application server
All communication happens between servers via kafka messaging queue

"""

# Importing all the modules
import os
import time
import logging
import json
from model.model import check_model_parts
from model.splitModel import split_model
from kakfa_handler.kafka_handler import KafkaHandler
from mongo_db.mongo_db import MongoDB

from Logger.formatter import CustomFormatter

# Getting logger object
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Setting logger level to INFO

# Creating stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)  # Setting stream handler level to DEBUG

ch.setFormatter(CustomFormatter())  # Setting formatter for stream handler
logger.addHandler(ch)

if __name__ == "__main__":
    logger.info("Checking model files and connecting to Kafka...")

    # Ensure all required model files are present before starting the Kafka consumer
    while not check_model_parts(["model.h5", "model_part1.h5", "model_part2.h5", "model_part3.h5"]):
        logger.info("Required model files are missing. Running split_model.py to generate model parts.")
        split_model()
        logger.info("Checking again in 30 seconds...")
        time.sleep(30)

    logger.info("All model files are present")

    # Creating MongoDB client instance
    mongo_cli = MongoDB()
    # Creating Kafka handler instance
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    # Checking if output_part1 file exists in GridFS
    if not mongo_cli.file_in_gridfs("output_part1.txt"):
        logger.info("Running server1.py to generate output_1.txt")
        logger.info("Sending partial-inference-1 message")
        # Sending Kafka message to server 1 for the initial model inference
        producer.send("partial-inference-1", value=json.dumps({"inference_exists": "False"}))
        logger.info("Subscribing to partial-inference-1 topic")
        consumer.subscribe("partial-inference-1") # Subscribing to Kafka topic
        didConsumeMessage = False
        while not didConsumeMessage:
            message = consumer.poll(6000) # Polling Kafka consumer
            if message is None:
                continue
            for topic, messages in message.items():
                for message in messages:
                    json_value = json.loads(message.value)
                    if json_value['inference_exists'] == "True":
                        logger.info("Inference 1 message received")
                        didConsumeMessage = True
                        break

            # consumer.close()
            if didConsumeMessage:
                break

    # Checking if output_part2 file exists in GridFS
    if not mongo_cli.file_in_gridfs("output_part2.txt"):
        logger.info("Running server2.py to generate output_2.txt")
        logger.info("Sending partial-inference-2 message")
        # Sending Kafka message to server 2 for the second model inference
        producer.send("partial-inference-2", value=json.dumps({"inference_exists": "False"}))
        logger.info("Subscribing to partial-inference-2 topic")
        consumer.subscribe("partial-inference-2")  # Subscribing to Kafka topic
        didConsumeMessage = False
        while not didConsumeMessage:
            message = consumer.poll(6000)   # poll the consumer
            if message is None:
                continue
            for topic, messages in message.items():
                for message in messages:
                    json_value = json.loads(message.value)
                    if json_value['inference_exists'] == "True":
                        logger.info("Inference 2 message received")
                        didConsumeMessage = True
                        break
            # consumer.close()
            if didConsumeMessage:
                break

    consumer.subscribe(topics=["song-ids", "recommendation"])

    while True:
        message = consumer.poll(3000)

        if message is None:
            continue

        # Iterating over topics and messages
        for topic, messages in message.items():
            for message in messages:
                count = 0
                if topic.topic == "song-ids":
                    logger.info("Received song-ids message")
                    songs = []
                    for song in message.value:
                        songs.append(song['trackId'])
                    logger.info("sending final-recommendation message")
                    producer.send("partial-inference-3", value=','.join(songs))

                elif topic.topic == "recommendation":
                    logger.info("Received recommendation message")
                    finalResult = json.loads(message.value)
                    logger.info(finalResult)
                    logger.info("Sending final-recommendation message")
                    producer.send("final-recommendation", value=finalResult)

    consumer.close() # Closing Kafka consumer
