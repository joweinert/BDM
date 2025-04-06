from pyspark.sql import SparkSession
import os
import json
import uuid
import boto3
from io import BytesIO
from datetime import datetime, timezone
from pyspark.sql.utils import AnalysisException


class DeltaStorageHandler:
    def __init__(self, storage_path="Landing_Zone"):
        """
        Initialize the DeltaStorageHandler for storing data in MinIO (S3).

        Args:
            minio_bucket (str): MinIO bucket name.
            storage_path (str): Path inside the bucket for storing Delta tables.
        """
        # Use MINIO_DATA_BUCKET environment variable, defaulting to "mybucket" if not provided
        self.minio_bucket = os.getenv("MINIO_DATA_BUCKET")
        self.minio_unstructured = os.getenv("MINIO_UNSTRUCTURED_BUCKET")
        self.storage_path = storage_path.strip("/")

        # Read MinIO credentials from environment variables
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT")
        self.access_key = os.getenv("MINIO_ROOT_USER")
        self.secret_key = os.getenv("MINIO_ROOT_PASSWORD")

        self.s3_client = boto3.client(
            "s3",
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
        )

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

    @staticmethod
    def _construct_table_name(zone, datasource, dataset):
        """Returns a formatted table name."""
        if zone == "Landing":
            return f"{datasource}/{dataset}/{datetime.now(timezone.utc).strftime('%d%m%Y_%H%M%S')}"
        elif zone == "Trusted":
            return f"{datasource}/{dataset}/{datetime.now(timezone.utc).strftime('%d%m%Y_%H%M%S')}"
        elif zone == "Exploitation":
            return f"{datasource}/{dataset}/{datetime.now(timezone.utc).strftime('%d%m%Y_%H%M%S')}"
        raise ValueError(
            "Invalid zone. Valid zones are: Landing, Trusted, Exploitation."
        )

    def set_storage_path(self, storage_path):
        """Sets the storage path inside the MinIO bucket."""
        self.storage_path = storage_path.strip("/")

    def list_tables(self):
        """
        Lists all Delta tables in the current storage path on MinIO by looking for keys containing '_delta_log/'.

        Returns:
            List[str]: List of table paths relative to the storage path.
        """
        prefix = f"{self.storage_path}/"
        paginator = self.s3_client.get_paginator("list_objects_v2")
        tables = set()

        for page in paginator.paginate(Bucket=self.minio_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "_delta_log/" in key:
                    table_path = key.split("/_delta_log")[0]
                    relative_path = table_path[len(prefix) :]
                    print(f"🔹 Found Delta table at: {relative_path}")
                    tables.add(relative_path)

        return list(tables)

    def write_relational_data(
        self, rows, columns, datasource, dataset, zone="Landing", mode="append"
    ):
        """
        Converts a list of rows + column names from a relational DB into a Spark DataFrame
        and stores it as a Delta table.

        Args:
            rows (List[Tuple]): Raw data rows fetched from a DB.
            columns (List[str]): Corresponding column names.
            datasource (str): Identifier for the source system.
            dataset (str): Logical dataset/table name.
            zone (str): Data zone (Landing, Trusted, Exploitation).
            mode (str): Write mode for Delta ("append" or "overwrite").
        """
        if not rows:
            print(f"⚠️ No data provided for {datasource}.{dataset}")
            return None

        df = self.spark.createDataFrame(rows, schema=columns)
        table_name = self._construct_table_name(zone, datasource, dataset)
        delta_path = self._get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(
            delta_path
        )
        print(f"✅ Relational DB data saved to {delta_path}")
        return table_name

    def write_csv(self, csv_path, datasource, dataset, zone="Landing", mode="append"):
        """
        Reads a CSV file and stores it as a Delta table.

        Args:
            csv_path (str): Path to the CSV file.
            datasource (str): Identifier for the source system.
            dataset (str): Logical dataset/table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        df = self.spark.read.option("header", "true").csv(csv_path)
        table_name = self._construct_table_name(zone, datasource, dataset)
        delta_path = self._get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(
            delta_path
        )
        print(f"✅ CSV data saved to {delta_path}")
        return table_name

    def write_json(self, json_path, datasource, dataset, zone="Landing", mode="append"):
        """
        Reads a JSON file and stores it as a Delta table.

        Args:
            json_path (str): Path to the JSON file.
            datasource (str): Identifier for the source system.
            dataset (str): Logical dataset/table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        df = self.spark.read.option("multiline", "true").json(json_path)
        table_name = self._construct_table_name(zone, datasource, dataset)
        delta_path = self._get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(
            delta_path
        )
        print(f"✅ JSON data saved to {delta_path}")
        return table_name

    def write_api_json(
        self, api_data, datasource, dataset, zone="Landing", mode="append"
    ):
        """
        Writes a JSON object (e.g., an API response) as a Delta table.

        Args:
            api_data (dict or list): JSON data obtained from an API.
            datasource (str): Identifier for the source system.
            dataset (str): Logical dataset/table name.
            mode (str): Write mode ("append" or "overwrite").
        """
        # Convert the API data to a JSON string and parallelize it into an RDD
        rdd = self.spark.sparkContext.parallelize([json.dumps(api_data)])
        df = self.spark.read.json(rdd)
        table_name = self._construct_table_name(zone, datasource, dataset)
        delta_path = self._get_delta_path(table_name)
        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(
            delta_path
        )
        print(f"✅ API JSON data saved to {delta_path}")
        return table_name

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

    def upload_image(
        self,
        short_desc,
        image_obj,
        datasource,
        dataset,
        zone="Landing",
        format="JPEG",
        tags=None,
    ):
        """
        Uploads an in-memory image to MinIO, extracts metadata, and stores it in Delta Lake.

        Args:
            metadata_table (str): Delta table name for storing metadata in deltalake.
            short_desc (str): short image description of the image file.
            image_obj (PIL.Image.Image): In-memory image object.
            format (str): Image format (JPEG or PNG).
            tags (list): Optional tags to associate with the image.

        Returns:
            dict: Metadata of the uploaded image.
        """
        if format not in ["JPEG", "PNG"]:
            raise ValueError("Image format must be JPEG or PNG.")
        else:
            ext_map = {"JPEG": "jpg", "PNG": "png"}

        image_id = str(uuid.uuid4())
        filename = f"{image_id}.{ext_map.get(format)}"

        width, height = image_obj.size

        # Convert PIL image to BytesIO
        with BytesIO() as buffer:
            image_obj.save(buffer, format=format)
            buffer.seek(0)
            size_bytes = len(buffer.getvalue())

            # Upload to MinIO
            self.s3_client.upload_fileobj(
                buffer, self.minio_unstructured, "images/" + filename
            )
            s3_path = f"s3a://{self.minio_unstructured}/images/{filename}"

        metadata = {
            "id": image_id,
            "filename": filename,
            "short_desc": short_desc,
            "s3_path": s3_path,
            "upload_time": datetime.now(timezone.utc).isoformat(),
            "size_bytes": size_bytes,
            "format": format,
            "width": width,
            "height": height,
            "tags": tags or [],
        }
        self.write_api_json(metadata, datasource, dataset, zone)
        print("✅ Image uploaded and metadata stored in Delta Lake")
        return metadata

    def get_max_column_value(self, table_name: str, column: str):
        """
        Returns the max value in a column from a Delta table (e.g. latest year).

        Args:
            table_name (str): Delta table name.
            column (str): Column to find the max value from.

        Returns:
            max value (int/float/str) or None if table doesn't exist or is empty
        """
        delta_path = self._get_delta_path(table_name)
        try:
            df = self.spark.read.format("delta").load(delta_path)
            if column not in df.columns:
                print(f"⚠️ Column '{column}' does not exist in table '{table_name}'.")
                return None

            dtype = dict(df.dtypes).get(column)
            if dtype in ["struct", "map", "array"]:
                print(f"⚠️ Unsupported column type '{dtype}' for max operation.")
                return None

            max_row = df.selectExpr(f"max({column}) as max_value").collect()[0]
            return max_row["max_value"]
        except AnalysisException:
            print(f"⚠️ Table {table_name} not found.")
            return None
        except Exception as e:
            print(f"❌ Error reading max value: {e}")
            return None
