from pyspark.sql import SparkSession
import os
import json
import uuid
import boto3
from io import BytesIO
from datetime import datetime
from PIL import Image
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, ArrayType, BinaryType


class DeltaStorageHandler:
    def __init__(self, storage_path=""):
        """
        Initialize the DeltaStorageHandler for storing data in MinIO (S3).

        Args:
            minio_bucket (str): MinIO bucket name.
            storage_path (str): Path inside the bucket for storing Delta tables.
        """
        # Use MINIO_DATA_BUCKET environment variable, defaulting to "mybucket" if not provided
        self.minio_bucket = os.getenv("MINIO_DATA_BUCKET")
        self.minio_pipeline = os.getenv("MINIO_SCRIPT_BUCKET")
        self.storage_path = storage_path.strip("/")

        # Read MinIO credentials from environment variables
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")

        # Initialize Spark with Delta support
        self.spark = (
            SparkSession.builder.appName("DeltaStorageHandler")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.endpoint", self.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", self.access_key)
            .config("spark.hadoop.fs.s3a.secret.key", self.secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0")
            .getOrCreate()
        )

    def _get_delta_path(self, table_name):
        """Returns the S3 path for a given Delta table."""
        return f"s3a://{self.minio_bucket}/{self.storage_path}/{table_name}"

    def _get_pipeline_path(self, table_name):
        """Returns the S3 path for a given Delta table."""
        return f"s3a://{self.minio_pipeline}/{self.storage_path}/{table_name}"

    def write_csv(self, csv_path, table_name, mode="append"):
        """
        Reads a CSV file and stores it as a Delta table.

        Args:
            csv_path (str): Path to the CSV file.
            table_name (str): Delta table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        df = self.spark.read.option("header", "true").csv(csv_path)
        delta_path = self._get_delta_path(table_name)

        df.write.format("delta").mode(mode).save(delta_path)
        print(f"✅ CSV data saved to {delta_path}")

    def write_json(self, json_path, table_name, mode="append"):
        """
        Reads a JSON file and stores it as a Delta table.

        Args:
            json_path (str): Path to the JSON file.
            table_name (str): Delta table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        df = self.spark.read.option("multiline", "true").json(json_path)
        delta_path = self._get_delta_path(table_name)

        df.write.format("delta").mode(mode).save(delta_path)
        print(f"✅ JSON data saved to {delta_path}")

    def write_api_json(self, api_data, table_name, mode="append"):
        """
        Writes a JSON object (e.g., an API response) as a Delta table.

        Args:
            api_data (dict or list): JSON data obtained from an API.
            table_name (str): Delta table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        # Convert the API data to a JSON string and parallelize it into an RDD
        rdd = self.spark.sparkContext.parallelize([json.dumps(api_data)])
        df = self.spark.read.json(rdd)
        delta_path = self._get_delta_path(table_name)
        df.write.format("delta").mode(mode).save(delta_path)
        print(f"✅ API JSON data saved to {delta_path}")

    def read_table(self, table_name):
        """
        Reads a Delta table from MinIO.

        Args:
            table_name (str): Name of the Delta table to read.

        Returns:
            Spark DataFrame
        """
        delta_path = self._get_delta_path(table_name)
        df = self.spark.read.format("delta").load(delta_path)
        return df

    def show_table(self, table_name, num_rows=5):
        """
        Displays data from a Delta table.

        Args:
            table_name (str): Name of the Delta table.
            num_rows (int): Number of rows to display.
        """
        df = self.read_table(table_name)
        df.show(num_rows)

    def stop_spark(self):
        """Stops the Spark session."""
        self.spark.stop()
        print("✨ Spark session stopped.")

    def upload_image(self, image_name, image_obj, metadata_table, tags=None):
        """
        Uploads an in-memory image to MinIO, extracts metadata, and stores it in Delta Lake.

        Args:
            image_name (str): Name of the image file (e.g., "cat.png").
            image_obj (PIL.Image.Image): In-memory image object.
            metadata_table (str): Delta table name for storing metadata and image data.
            tags (list): Optional tags to associate with the image.

        Returns:
            dict: Metadata of the uploaded image.
        """
        # Generate a unique ID for the image
        image_id = str(uuid.uuid4())
        ext = os.path.splitext(image_name)[1]
        object_name = f"{image_id}{ext}"  # Store image under the metadata_table path
        
        # Save the in-memory image to a temporary buffer
        #buffer = image_obj.fp if hasattr(image_obj, "fp") else BytesIO()
        #image_obj.save(buffer, format=image_obj.format or "JPEG")
        #buffer.seek(0)

        # Initialize MinIO (S3) client
        s3_client = boto3.client(
            "s3",
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

        # Extract the bucket name and object key
        bucket_name = self.minio_bucket
        object_key = f"{metadata_table}/{object_name}"

        # Upload the image to MinIO
        s3_client.upload_fileobj(image_obj, bucket_name, object_key)
        s3_path = f"s3a://{bucket_name}/{object_key}"

        # Extract metadata
        width, height = image_obj.size
        format = image_obj.format.lower() if image_obj.format else "unknown"
        size_bytes = image_obj.tell()

        # Encode the image as binary data
        #buffer.seek(0)
        image_binary = image_obj.read()

        # Create metadata dictionary
        metadata = {
            "id": image_id,
            "filename": image_name,
            "s3_path": s3_path,
            "upload_time": datetime.utcnow().isoformat(),
            "size_bytes": size_bytes,
            "format": format,
            "width": width,
            "height": height,
            "tags": tags or [],
            "image_data": image_binary,  # Store the binary image data
        }

        # Store metadata and image data in Delta Lake
        self._store_metadata_in_delta(metadata, metadata_table)

        print(f"✅ Image uploaded and metadata stored in Delta Lake: {s3_path}")
        return metadata

def _store_metadata_in_delta(self, metadata, table_name, mode="append"):
    """
    Stores metadata and image data in a Delta table.

    Args:
        metadata (dict): Metadata dictionary to store.
        table_name (str): Delta table name.
        mode (str): Write mode ("append" or "overwrite").
    """
    try:      
        # Read the RDD as a DataFrame
        df = self.spark.read.json(metadata)
        
        # Get the Delta table path
        delta_path = self._get_delta_path(table_name)
        
        # Write the DataFrame to the Delta table
        df.write.format("delta").mode(mode).save(delta_path)
        print(f"✅ Metadata written to Delta table at {delta_path}")
    except Exception as e:
        print(f"❌ Failed to write metadata to Delta table: {e}")