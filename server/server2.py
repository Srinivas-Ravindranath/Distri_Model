"""
This script runs the second partial part of the model inference
This save the inference output to the database and
communicate with the dispatcher node via kafka


"""

# Import Required Modules
import json
import logging
import numpy as np
from tensorflow.keras.models import load_model
from mongo_db.mongo_db import MongoDB
from kakfa_handler.kafka_handler import KafkaHandler

from Logger.formatter import CustomFormatter

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creating stream handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

def inference_part_2():
    # Load the partial RNN model
    model_part2 = load_model('RnnModel/model_part2.h5')

    mongo_cli = MongoDB()

    # Reading the initial inference from mongoDB
    data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part1.txt')
    input_data = np.loadtxt(data_buffer)
    # Run the partial inference anf get the required partial results
    output_part2 = model_part2.predict(input_data)
    np.savetxt('output_part2.txt', output_part2)

    # Saving this partial result in mongoDB GRIDFS
    mongo_cli.add_file_to_gridfs(file_path='output_part2.txt')


def process_kafka_messages():
    # Initialize Kafka Producer and Consumer Queues
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()
    # Subscribe to the consumer topic
    consumer.subscribe(topics=["partial-inference-2"])
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
                    logger.info("Running inference part 2")
                    inference_part_2()
                    logger.info("Inference part 2 complete")
                    producer.send("partial-inference-2", value=json.dumps({"inference_exists": "True"}))
                    logger.info("Sent partial-inference-2 message")
    consumer.close()


process_kafka_messages()