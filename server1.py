import socket
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import load_model

from load import load_data
import threading

# Load the TensorFlow model
model_part1 = load_model('RnnModel/model_part1.h5')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def kafka_data_processor():
    # Kafka consumer configuration
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')

    output_part1 = model_part1.predict(artist_indices.reshape(-1, 1))
    concatenated_inputs = np.concatenate([output_part1, X], axis=1)
    np.savetxt('output_part1.txt', concatenated_inputs)



        # Process the input data

        # print("Full predictions shape:", concatenated_inputs)
        # print("Full predictions shape:", concatenated_inputs.shape)  # Debugging
        # Convert prediction result to list and serialize to JSON
        # with open("output_part1.txt", "w") as file:
        #     file.write(str(concatenated_inputs))
        # Send the output_part1 back to dispatcher or to the next server via Kafka

        # Optionally, send data back through the socket to a connected client
        # client_socket.sendall(result_data.encode('utf-8'))


# def run_server(port):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
#         server_socket.bind(('localhost', port))
#         server_socket.listen(1)
#         print(f"Server listening on port {port}")
#
#         while True:
#             client_socket, addr = server_socket.accept()
#             print('Connected by', addr)
#
#             # Handle each client connection in a separate thread
#             client_thread = threading.Thread(target=kafka_data_processor, args=(client_socket,))
#             client_thread.start()


if __name__ == "__main__":
    # run_server(8000)
    kafka_data_processor()
