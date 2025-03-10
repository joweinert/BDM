from pyspark.sql import SparkSession
import os
import json


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
