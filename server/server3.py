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

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

def run_training_part_3():

    mongo_cli = MongoDB()
    model_part3 = load_model('RnnModel/model_part3.h5')

    if not mongo_cli.file_in_gridfs("output_part3.txt"):
        data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part2.txt')
        input_data = np.loadtxt(data_buffer)
        predictions = model_part3.predict(input_data)
        np.savetxt('output_part3.txt', predictions)
        mongo_cli.add_file_to_gridfs(file_path='output_part3.txt')

    data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part3.txt')
    input_data = np.loadtxt(data_buffer)
    return input_data


def process_kafka_messages():
    prediction = run_training_part_3()
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["partial-inference-3"])
    while True:
        message = consumer.poll(3000)
        if message is None:
            continue
        for topic, messages in message.items():
            for message in messages:
                logger.info("Received final-recommendation message")
                logger.info(message.value)
                track_id_list = message.value.split(",")
                results = get_reccomendations(predictions=prediction, track_id_list=track_id_list)
                logger.info("final results: ", results)
                producer.send("recommendation", value=json.dumps(results))
                logger.info("Sent recommendation message")
    consumer.close()


# Run the function
process_kafka_messages()
