from kafka import KafkaConsumer
import json
import base64
from PIL import Image
from io import BytesIO
import os

# Kafka Configuration
KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
TOPIC_NAME = "image_stream"

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
)

# Consume messages
for message in consumer:
    image_name = message.value["image_name"]
    image_data = message.value["image_data"]

    # Decode the image
    image_binary = base64.b64decode(image_data)
    image = Image.open(BytesIO(image_binary))

    # Display the image (optional)
    image.show()

    print(f"Consumed image: {image_name}")
