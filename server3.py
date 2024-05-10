import socket
import json
import numpy as np
from tensorflow.keras.models import load_model
from mongo_db import MongoDB
from kafka_handler import KafkaHandler
from recomendation import get_reccomendations


def run_training_part_3():

    mongo_cli = MongoDB()
    model_part3 = load_model('RnnModel/model_part3.h5')

    # if not mongo_cli.file_in_gridfs("output_part3.txt"):
    #     data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part2.txt')
    #     input_data = np.loadtxt(data_buffer)
    #     predictions = model_part3.predict(input_data)
    #     np.savetxt('output_part3.txt', predictions)
    #     mongo_cli.add_file_to_gridfs(file_path='output_part3.txt')

    # data_buffer = mongo_cli.read_file_from_gridfs(file_name='output_part3.txt')
    # input_data = np.loadtxt(data_buffer)
    input_data = np.loadtxt('output_part3.txt')
    #final_prediction = model_part3.predict(input_data)
    return input_data


def process_kafka_messages():
    prediction = run_training_part_3()
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    consumer.subscribe(topics=["final-recommendation"])
    while True:
        message = consumer.poll(3000)
        if message is None:
            continue
        print(message.items())
        for topic, messages in message.items():
            for message in messages:
                print("Received final-recommendation message")
                print(message.value)
                track_id_list = message.value.split(",")
                results = get_reccomendations(predictions=prediction, track_id_list=track_id_list)
                print("final results: ", results)
                producer.send("recommendation", value=json.dumps(results))
    consumer.close()


# Run the function
process_kafka_messages()
