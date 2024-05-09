# from kafka import KafkaConsumer
# from kafka import KafkaProducer
# import json
#
#
# def process_kafka_messages(kafka_topic, key) -> str:
#     # Kafka consumer configuration
#     KAFKA_BROKER_URL = 'localhost:9092'  # Adjust as per your Kafka broker config
#
#     # Create a Kafka consumer
#     consumer = KafkaConsumer(
#         kafka_topic,
#         bootstrap_servers=[KAFKA_BROKER_URL],
#         auto_offset_reset='earliest',  # Start reading at the earliest message
#         value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None  # Deserialize JSON if not empty
#     )
#     # List to store all track IDs
#     track_ids = []
#
#     # Process messages from Kafka
#     try:
#         for message in consumer:
#             message_val = message.value
#             if message_val is not None:
#                 # Iterate through each entry in the received message
#                 for each in message_val:
#                     print(each)
#                     if key in each:
#                         track_id = each(key)
#                         track_ids.append(track_id)
#                         print(f"Track IDs: {track_id}")
#                     else:
#                         print("No trackids found in this message")
#             else:
#                 print("Received an empty or invalid message")
#
#     except KeyboardInterrupt:
#         print("Stopped by user.")
#     except json.JSONDecodeError as e:
#         print("Failed to decode JSON:", e)
#
#     return ','.join(track_ids)
#     # Send processed track IDs to Kafka producer
#     # kafkaProducer(track_ids)
#
# def kafkaProducer(track_ids):
#     # Kafka producer configuration
#     KAFKA_TOPIC = 'song-ids'
#     KAFKA_BROKER_URL = 'localhost:9092'  # Adjust as per your Kafka broker config
#
#     # Create a Kafka producer
#     producer = KafkaProducer(
#         bootstrap_servers=[KAFKA_BROKER_URL],
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON data
#     )
#
#     # Send messages to Kafka
#     try:
#         message = {'trackId': track_ids}
#         producer.send(KAFKA_TOPIC, value=message)
#         print(f"Sent processed track IDs: {track_ids}")
#     except KeyboardInterrupt:
#         print("Stopped by user.")
#     finally:
#         producer.flush()
#         producer.close()