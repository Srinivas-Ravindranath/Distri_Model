import socket
import json
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import load_model

from load import load_data
import threading

# Load the TensorFlow model
model_part1 = load_model('RnnModel/model_part1.h5')
model_part3 = load_model('RnnModel/model_part3.h5')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def kafka_data_processor():
    # Kafka consumer configuration
    df, X, artist_indices, num_artists = load_data('Dataset/spotify_data.csv')
    output_part2 = np.loadtxt('output_part2.txt')
    predictions = model_part3.predict(output_part2)
    np.savetxt('output_part3.txt', predictions)

    # # Check if selected_indices is not empty and correctly formed
    # if len(selected_indices) == 0 or selected_indices is None:
    #     print("No valid selected indices.")
    #     return pd.DataFrame()
    #
    # # Attempt to get a proper slice of predictions
    # try:
    #     selected_predictions = predictions[selected_indices, :]
    # except IndexError:
    #     print("IndexError with selected_indices:", selected_indices)
    #     return pd.DataFrame()
    #
    # print("Shape of selected predictions:", selected_predictions.shape)
    #
    # if selected_predictions.ndim == 1:  # If still 1D, reshape to maintain two dimensions
    #     selected_predictions = selected_predictions.reshape(1, -1)
    #
    # avg_selected_predictions = np.mean(selected_predictions, axis=0)
    # print("Shape of average selected predictions:", avg_selected_predictions.shape)
    #
    # # Ensure avg_selected_predictions is a 2-dimensional array
    # avg_selected_predictions_2d = avg_selected_predictions.reshape(1, -1)
    # print("Shape of avg_selected_predictions_2d:", avg_selected_predictions_2d.shape)
    #
    # # Calculate the similarity
    # try:
    #     distances = cdist(avg_selected_predictions_2d, predictions, metric='euclidean')
    #     print("Successfully calculated distances.")
    # except ValueError as e:
    #     print("Error in calculating distances:", e)
    #
    #
    # recommended_indices = np.argsort(distances[0])[:top_n + len(selected_indices)]
    # recommended_indices = [idx for idx in recommended_indices if idx not in selected_indices][:top_n]
    #
    #  print(df.iloc[recommended_indices][['artist_name', 'track_name', 'track_id']])


if __name__ == "__main__":
    # run_server(8000)
    kafka_data_processor()
