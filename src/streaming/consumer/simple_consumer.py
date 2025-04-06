from kafka import KafkaConsumer
import json
import os

KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
TOPICS = [
    "crypto_stream",
    "fraud_detect",
    "stock_stream",
]

consumer = KafkaConsumer(
    *TOPICS,  # subscribe to all topics in the list
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

print("Starting Kafka Consumer...")

for message in consumer:
    print(f"[{message.topic}]: {message.value}")
