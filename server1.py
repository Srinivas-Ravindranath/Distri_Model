import numpy as np

from mongo_db import MongoDB

from load import load_data

from kafka_handler import KafkaHandler
from tensorflow.keras.models import load_model

import json


def run_training_part_1():
    # Load the TensorFlow model
    model_part1 = load_model('RnnModel/model_part1.h5')
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')

    output_part1 = model_part1.predict(artist_indices.reshape(-1, 1))
    concatenated_inputs = np.concatenate([output_part1, X], axis=1)
    np.savetxt('output_part1.txt', concatenated_inputs)
    mongo_cli = MongoDB()
    mongo_cli.add_file_to_gridfs(file_path='output_part1.txt')

    kafka_handler = KafkaHandler(
        kafka_topic='partial_inference',
        key='inference_exists',
        kafka_producer_topic='partial_inference',
        kafka_producer_value=json.dumps({'inference_exists': True}).encode('utf-8')
    )

    kafka_handler.process_kafka_messages()

