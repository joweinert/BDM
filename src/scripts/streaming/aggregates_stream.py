import os
import traceback
from util.delta_storage import DeltaStorageHandler
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col, window, avg, sum as spark_sum
from typing import Optional, List
from schema import get_schema

def stream_aggregates_from_kafka(
        handler,
        kafka_broker: str,
        topic: str,
        datasource: str,
        dataset: str,
        zone: str = "Landing",
        starting_offsets: str = "latest",
        aggregation_window: str = "1 minute",  # window duration
        checkpoint_path: Optional[str] = None,
        storage_path: Optional[str] = None,
        partition_by: Optional[List[str]] = None,
        merge_schema: bool = True,
        mode: str = "complete"  # usually aggregation uses 'complete' mode
    ):
        """
        Streams data from Kafka, aggregates it over time windows, and writes to Delta.

        Args:
            kafka_broker (str): Kafka bootstrap servers.
            topic (str): Kafka topic to subscribe to.
            datasource (str): Logical source system name.
            dataset (str): Logical dataset name.
            zone (str): Data zone ("Landing", "Trusted", etc.).
            starting_offsets (str): Kafka offset starting point.
            aggregation_window (str): Duration of aggregation window (e.g., "1 minute").
            checkpoint_path (str): Path for streaming checkpoint.
            storage_path (str): Path to store aggregated Delta data.
            partition_by (List[str]): Partition columns for Delta.
            merge_schema (bool): Whether to allow Delta schema evolution.
            mode (str): Write mode, typically "complete" for aggregations.

        Returns:
            StreamingQuery object
        """

        # Get schema
        schema: StructType = get_schema(datasource, dataset)
    
        # Read Kafka stream
        kafka_df = handler.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .load()

        # Parse JSON from Kafka value
        parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        # Ensure timestamp is timestamp type
        # (Assuming your schema has a field named 'timestamp' as string or timestamp)
        # If timestamp is string, cast it:
        parsed_df = parsed_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        # Aggregate with watermark and window
        aggregated_df = parsed_df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                col("symbol"),
                window(col("timestamp"), aggregation_window)
            ) \
            .agg(
                avg("current_price").alias("avg_price"),
                spark_sum("volume").alias("total_volume"),
                # Add more aggregations as needed
            ) \
            .select(
                col("symbol"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("avg_price"),
                col("total_volume")
            )

        # Construct table and delta path
        table_name = handler._construct_table_name(zone, datasource, dataset) + "_aggregated"
        delta_path = handler.get_delta_path(table_name, storage_path)

        # Define checkpoint path if not set
        if checkpoint_path is None:
            checkpoint_path = f"s3a://{handler.minio_bucket}/_checkpoints/{table_name.replace('/', '_')}"

        # Write aggregated stream to Delta
        writer = aggregated_df.writeStream.format("delta") \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode(mode)

        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        query = writer.start(delta_path)
        print(f"🚀 Aggregated streaming started from Kafka topic '{topic}' to Delta path: {delta_path}")
        return query

if __name__ == "__main__":
    try:
        KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
        print(f"Using Kafka broker at: {KAFKA_BROKER}")

        TOPICS = [
        #"crypto_stream"
        #"fraud_detect",
        "stock_stream",
        ]

        print("Starting Kafka Consumer...")
        handler = DeltaStorageHandler()
        query = stream_aggregates_from_kafka(
            handler = handler,
            kafka_broker=KAFKA_BROKER,
            topic="stock_stream",
            datasource="stock_stream",
            dataset="AAPL",
            zone="Landing",
            starting_offsets="latest",
            aggregation_window= "5 minutes",  # window duration
            checkpoint_path=None,
            storage_path="Landing_Zone",
            partition_by=None,
            merge_schema=True,
            mode="complete"
        )

        # Keep the streaming query running
        query.awaitTermination()

    except Exception as e:
        print("Exception occurred during streaming:")
        traceback.print_exc()
