import os
import time
import json
from model.model import check_model_parts
from model.splitModel import split_model
from kakfa_handler.kafka_handler import KafkaHandler
from mongo_db.mongo_db import MongoDB

if __name__ == "__main__":
    print("Checking model files and connecting to Kafka...")

    # Ensure all required model files are present before starting the Kafka consumer
    while not check_model_parts(["model.h5", "model_part1.h5", "model_part2.h5", "model_part3.h5"]):
        print("Required model files are missing. Running split_model.py to generate model parts.")
        split_model()
        print("Checking again in 30 seconds...")
        time.sleep(30)

    print("All model files are present")

    mongo_cli = MongoDB()
    kafka_handler = KafkaHandler()
    consumer = kafka_handler.initialize_kafka_consumer()
    producer = kafka_handler.initialize_kafka_producer()

    if not mongo_cli.file_in_gridfs("output_part1.txt"):
        print("Running server1.py to generate output_1.txt")
        data = json.dumps({'inference_exists': "False"})
        producer.send("partial_inference_1", value=data.encode('utf-8'))
        # run_training_part_1()

    if not mongo_cli.file_in_gridfs("output_part2.txt"):
        print("Running server2.py to generate output_2.txt")
        producer.send("partial_inference_2", value=json.dumps({'inference_exists': "False"}).encode('utf-8'))
        # run_training_part_2()

    # if not mongo_cli.file_in_gridfs("output_part3.txt"):
    #     print("Running server2.py to generate output_3.txt")
    #     run_training_part_3()

    consumer.subscribe(topics=["song-ids", "recommendation"])

    while True:
        message = consumer.poll(3000)

        if message is None:
            continue

        for topic, messages in message.items():
            for message in messages:
                if topic.topic == "song-ids":
                    print("Received song-ids message")
                    songs = []
                    for song in message.value:
                        songs.append(song['trackId'])
                    producer.send("final-recommendation", value=','.join(songs))

                elif topic.topic == "recommendation":
                    print("Received recommendation message")
                    print(message.value)

    consumer.close()
