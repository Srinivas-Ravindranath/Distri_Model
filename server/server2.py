import socket
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import load_model
from mongo_db import MongoDB
from kafka_handler import KafkaHandler

from load import load_data
import threading


def inference_part_2():
    model_part2 = load_model('RnnModel/model_part2.h5')

    mongo_cli = MongoDB()

    data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part1.txt')
    input_data = np.loadtxt(data_buffer)
    output_part2 = model_part2.predict(input_data)
    np.savetxt('output_part2.txt', output_part2)

    mongo_cli.add_file_to_gridfs(file_path='output_part2.txt')


def process_kafka_messages():
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["partial_inference_2"])
    while True:
        message = consumer.poll(3000)
        if message is None:
            continue
        for topic, messages in message.items():
            for message in messages:
                if message.value['inference_exists'] == "False":
                    inference_part_2()
                    producer.send("partial_inference_2", value=json.dumps({'inference_exists': True}).encode('utf-8'))
    consumer.close()
