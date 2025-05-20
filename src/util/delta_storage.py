from pyspark.sql import SparkSession
import os
import json
import uuid
import boto3
from io import BytesIO
from datetime import datetime, timezone
from pyspark.sql.utils import AnalysisException
from typing import Optional, List


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
    
    def get_delta_path(self, table_name, storage_path=None):
        """Returns the S3 path for a given Delta table, possibly with a custom storage path."""
        if storage_path is None:
            return f"s3a://{self.minio_bucket}/{self.storage_path}/{table_name}"
        return f"s3a://{self.minio_bucket}/{storage_path}/{table_name}"

    def get_storage_path(self):
        """Returns the current storage path."""
        return self.storage_path

    def get_minio_bucket(self):
        """Returns the MinIO bucket name."""
        return self.minio_bucket

    def get_spark(self):
        """Returns the Spark session."""
        return self.spark

    @staticmethod
    def _construct_table_name(zone, datasource, dataset):
        """Returns a formatted table name."""
        if zone == "Landing":
            return f"{datasource}/{dataset}/{datetime.now(timezone.utc).strftime('%d%m%Y_%H%M%S')}"
        elif zone == "Trusted":
            return f"{datasource}/{dataset}"
        elif zone == "Exploitation":
            return f"{datasource}/{dataset}/{datetime.now(timezone.utc).strftime('%d%m%Y_%H%M%S')}"
        raise ValueError("Invalid zone. Valid zones are: Landing, Trusted, Exploitation.")

    def set_storage_path(self, storage_path):
        """Sets the storage path inside the MinIO bucket."""
        self.storage_path = storage_path.strip("/")

    def list_tables(self):
        """
        Lists all Delta tables in the current storage path on MinIO by looking for keys containing '_delta_log/'.

        Returns:
            List[str]: List of table paths relative to the storage path.
        """
        prefix = f"{self.storage_path}/" if self.storage_path != "" else ""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        tables = set()

        for page in paginator.paginate(Bucket=self.minio_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "_delta_log/" in key:
                    table_path = key.split("/_delta_log")[0]
                    relative_path = table_path[len(prefix) :]
                    tables.add(relative_path)
        unique_tables = set(tables)
        return list(unique_tables)

    def write_df(
        self,
        df,
        path: Optional[str] = None,
        zone: Optional[str] = None,
        datasource: Optional[str] = None,
        dataset: Optional[str] = None,
        storage_path: Optional[str] = None,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        merge_schema: bool = True,
    ):
        """Write a Spark DataFrame as a Delta table.

        Args:
            df: the DataFrame to write
            path: optional custom path for the Delta table
            zone: one of "Landing", "Trusted", "Exploitation"
            datasource: the source system (e.g., "ecb")
            dataset: the logical dataset/table name (e.g., "exchange_rates")
            storage_path: optional custom storage path overriding the default
            mode: one of "overwrite", "append", etc.
            partition_by: list of columns to partition by (optional)
            merge_schema: whether to enable Delta mergeSchema option
        """
        if not df:
            print(f"⚠️ No data provided for {datasource}.{dataset}")
            return None
        if not path and not (zone and datasource and dataset and storage_path):
            raise ValueError("Either path or zone, datasource, dataset and storage_path must be provided.")
        elif not path:
            table_name = self._construct_table_name(zone, datasource, dataset)
            path = self.get_delta_path(table_name, storage_path)

        writer = df.write.format("delta").mode(mode)
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(path)
        print(f"✅ DataFrame written to Delta table at {path}")
        return path

    def write_relational_data(self, rows, columns, datasource, dataset, zone="Landing", mode="append"):
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
        delta_path = self.get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(delta_path)
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
        delta_path = self.get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(delta_path)
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
        delta_path = self.get_delta_path(table_name)

        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(delta_path)
        print(f"✅ JSON data saved to {delta_path}")
        return table_name

    def write_api_json(self, api_data, datasource, dataset, zone="Landing", mode="append"):
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
        delta_path = self.get_delta_path(table_name)
        df.write.option("mergeSchema", "true").format("delta").mode(mode).save(delta_path)
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
        if not table_name.startswith("s3a://"):
            delta_path = self.get_delta_path(table_name)
        else:
            delta_path = table_name
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

    def upload_json(self, path: str, content: dict):
        """
        Uploads a JSON file to the MinIO bucket.

        Args:
            path (str): The S3 key (path inside the bucket).
            content (dict): The content to write as JSON.
        """
        body = json.dumps(content, indent=2).encode("utf-8")
        self.s3_client.put_object(Bucket=self.minio_bucket, Key=path, Body=body, ContentType="application/json")
        print(f"✅ Uploaded JSON to s3a://{self.minio_bucket}/{path}")

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
            self.s3_client.upload_fileobj(buffer, self.minio_unstructured, "images/" + filename)
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
        delta_path = self.get_delta_path(table_name)
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

    def is_table_exported_or_quarantine(self, table: str) -> bool:
        """Returns True if the Delta table at `path` has TBLPROPERTY exported='true'.

        Args:
            table (str): Delta table name. Relative to the storage path.

        Returns:
            bool: True if the table is exported, False otherwise.
        """
        if not table.startswith("s3a://"):
            table = self.get_delta_path(table)
        props_df = self.spark.sql(f"SHOW TBLPROPERTIES delta.`{table}`")
        props = {row.key: row.value for row in props_df.collect()}
        return props.get("exported", "false").lower() == "true" or props.get("quarantine", "false").lower() == "true"

    def set_table_exported(self, table: str, exported: bool = True) -> None:
        """Sets the TBLPROPERTY exported='true' on the Delta table at `path`.

        Args:
            table (str): Delta table name. Relative to the storage path.
            exported (bool): True to set the property, False to unset it.
        """
        if not table.startswith("s3a://"):
            table = self.get_delta_path(table)
        self.spark.sql(f"ALTER TABLE delta.`{table}` SET TBLPROPERTIES ('exported' = '{str(exported).lower()}')")

    def set_table_quarantine(self, table: str, quarantine:bool = True) -> None:
        """Sets the TBLPROPERTY quarantine='true' on the Delta table at `path`.

        Args:
            table (str): Delta table name. Relative to the storage path.
        """
        if not table.startswith("s3a://"):
            table = self.get_delta_path(table)
        self.spark.sql(f"ALTER TABLE delta.`{table}` SET TBLPROPERTIES ('quarantine' = '{str(quarantine).lower()}')")
