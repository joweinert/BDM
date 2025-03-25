import time
import json
import websocket
from kafka import KafkaProducer
import os
from PIL import Image
from io import BytesIO
import base64

# Kafka Configuration
KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
TOPIC_NAME = "image_stream"  # Kafka topic for image streaming

print(KAFKA_BROKER)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Function to encode an image as Base64
def encode_image(image_path):
    """Encodes an image as Base64."""
    with open(image_path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode("utf-8")
    return encoded_string

# Stream images iteratively
def stream_images(image_folder):
    """Streams images from a folder to Kafka."""
    for image_name in os.listdir(image_folder):
        if image_name.lower().endswith(('.png', '.jpg', '.jpeg')):  # Filter image files
            image_path = os.path.join(image_folder, image_name)
            print(f"Processing image: {image_name}")

            # Encode the image
            encoded_image = encode_image(image_path)

            # Create a message payload
            message = {
                "image_name": image_name,
                "image_data": encoded_image
            }

            # Send the message to Kafka
            producer.send(TOPIC_NAME, message)
            print(f"Sent image {image_name} to Kafka")

            # Simulate a delay for streaming
            time.sleep(1)

if __name__ == "__main__":
    print(f"Starting Kafka Producer for image streaming...")

    # Folder containing images to stream
    image_folder = "./images"  # Replace with the path to your image folder

    # Stream images to Kafka
    while True:
        stream_images(image_folder)
        time.sleep(1)