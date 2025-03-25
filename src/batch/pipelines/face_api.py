import os
from dotenv import load_dotenv
import shutil
from io import BytesIO
from PIL import Image, ImageDraw
from util.delta_storage import DeltaStorageHandler

def create_image(image_name="generated_image.jpg", size=(256, 256), color=(255, 0, 0)):
    """
    Creates an in-memory image using Pillow.

    Args:
        image_name (str): Name of the image file.
        size (tuple): Size of the image (width, height).
        color (tuple): RGB color of the image.

    Returns:
        tuple: A tuple containing the image name and the PIL.Image object.
    """
    # Create a blank image with the specified size and color
    image = Image.new("RGB", size, color)

    # Optionally, draw something on the image
    draw = ImageDraw.Draw(image)
    draw.text((10, 10), "Test Image", fill=(255, 255, 255))

    return image_name, image



    # Initialize DeltaStorageHandler
storage = DeltaStorageHandler()

    # Create an image programmatically
image_name, image_obj = create_image()

    # Specify the Delta table name

    # Upload the generated image and store metadata
metadata = storage.upload_image(image_name, image_obj, metadata_table = "image")
print(f"Uploaded {image_name} with metadata:")
print(metadata)

    # Stop the Spark session when done
storage.stop_spark()