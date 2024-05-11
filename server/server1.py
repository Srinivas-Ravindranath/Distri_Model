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

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

def inference_part_1():
    # Load the TensorFlow model
    model_part1 = load_model('RnnModel/model_part1.h5')
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')

    output_part1 = model_part1.predict(artist_indices.reshape(-1, 1))
    concatenated_inputs = np.concatenate([output_part1, X], axis=1)
    np.savetxt('output_part1.txt', concatenated_inputs)
    mongo_cli = MongoDB()
    mongo_cli.add_file_to_gridfs(file_path='output_part1.txt')


def process_kafka_messages():
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["partial-inference-1"])
    while True:
        message = consumer.poll(3000)
        if message is None:
            continue
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
    consumer.close()

process_kafka_messages()