import socket
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import load_model

from load import load_data
import threading

# Load the TensorFlow model
model_part1 = load_model('RnnModel/model_part1.h5')
model_part2 = load_model('RnnModel/model_part2.h5')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def kafka_data_processor():
    # Kafka consumer configuration
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')
    concatenated_inputs = np.loadtxt('output_part1.txt')
    output_part2 = model_part2.predict(concatenated_inputs)
    np.savetxt('output_part2.txt', output_part2)

if __name__ == "__main__":
    # run_server(8000)
    kafka_data_processor()
