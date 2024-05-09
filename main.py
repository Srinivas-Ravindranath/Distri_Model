import os
import time
from splitModel import split_model
from model import check_model_parts
from mongo_db import MongoDB
from server1 import run_training_part_1
from server2 import run_training_part_2
from server3 import run_training_part_3

from kafka_handler import KafkaHandler

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
    if not mongo_cli.file_in_gridfs("output_part1.txt"):
        print("Running server1.py to generate output_1.txt")
        run_training_part_1()

    if not mongo_cli.file_in_gridfs("output_part2.txt"):
        print("Running server2.py to generate output_2.txt")
        run_training_part_2()

    if not mongo_cli.file_in_gridfs("output_part3.txt"):
        print("Running server2.py to generate output_3.txt")
        run_training_part_3()

    kafka_handler = KafkaHandler()
