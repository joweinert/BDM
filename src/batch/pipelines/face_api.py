import os
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv
import zipfile
import shutil
from io import BytesIO
from PIL import Image
from util.delta_storage import DeltaStorageHandler

# Load .env file (if present)
load_dotenv()
#zip file? can we process right away? or how? weird 
os.environ["KAGGLE_CONFIG_DIR"] = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.kaggle"))

def fetch_celebA_images_as_objects(download_dir='./celeba_dataset'):
    """
    Downloads the CelebA dataset from Kaggle, extracts it, and returns images as in-memory objects.

    Parameters:
        download_dir (str): Directory to store the downloaded dataset zip file.
    
    Returns:
        list: List of tuples containing image file names and PIL.Image objects.
    """

    # Initialize the Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Dataset name on Kaggle
    dataset_name = "dansbecker/5-celebrity-faces-dataset"

    # Ensure the download directory exists
    os.makedirs(download_dir, exist_ok=True)

    # Download the dataset as a zip file
    zip_path = os.path.join(download_dir, "5-celebrity-faces-dataset.zip")
    print(f"Downloading the dataset to {zip_path}...")
    api.dataset_download_files(dataset_name, path=download_dir, unzip=False)

    # Extract images as in-memory objects
    print(f"Extracting images from the dataset...")
    images = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):  # Filter image files
                with zip_ref.open(file) as image_file:
                    # Load the image into a PIL.Image object
                    image = Image.open(BytesIO(image_file.read()))
                    images.append((file, image))

    # Remove the zip file after processing
    os.remove(zip_path)

    return images


def extract_metadata(img):
    """
    Extracts image metadata such as size, format, width, and height from a PIL.Image object.

    Args:
        img (PIL.Image.Image): The image object to extract metadata from.

    Returns:
        tuple: A tuple containing width, height, format, and size in bytes.
    """
    width, height = img.size
    format = img.format.lower() if img.format else "unknown"

    # Get the size in bytes by saving the image to an in-memory buffer
    with BytesIO() as buffer:
        img.save(buffer, format=img.format or "JPEG")
        size_bytes = buffer.tell()

    return width, height, format, size_bytes
# Example usage:
# Example usage of extract_metadata
# Initialize DeltaStorageHandler
storage = DeltaStorageHandler()

# Fetch images as in-memory objects
images = fetch_celebA_images_as_objects()
if images:   # Upload each image and store metadata
    for image_name, image_obj in images:
        metadata = storage.upload_image(image_name, image_obj, "celeba_metadata", tags=["celeba", "face"])
        print(f"Uploaded {image_name} with metadata:")
        print(metadata)
else:
    print("Failed to fetch Celeb face images.")
# Stop the Spark session when done
storage.stop_spark()