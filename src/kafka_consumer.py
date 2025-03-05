from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "stock_data"

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

print("Starting Kafka Consumer...")

for message in consumer:
    stock_data = message.value
    print(f"Received Data: {stock_data}")
