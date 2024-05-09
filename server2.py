import socket
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import load_model
from mongo_db import MongoDB
from kafka_handler import KafkaHandler

from load import load_data
import threading


def run_training_part_2():
    model_part2 = load_model('RnnModel/model_part2.h5')

    mongo_cli = MongoDB()

    data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part1.txt')
    input_data = np.loadtxt(data_buffer)
    output_part2 = model_part2.predict(input_data)
    np.savetxt('output_part2.txt', output_part2)

    mongo_cli.add_file_to_gridfs(file_path='output_part2.txt')

    kafka_handler = KafkaHandler(
        kafka_topic='partial_inference',
        key='inference_exists',
        kafka_producer_topic='partial_inference',
        kafka_producer_value=json.dumps({'inference_exists': True}).encode('utf-8')
    )

    kafka_handler.process_kafka_messages()

