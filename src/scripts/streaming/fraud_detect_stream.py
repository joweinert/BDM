import os
import traceback
import random
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, BooleanType, StringType
from pyspark.sql.functions import from_json, col, udf, to_json, struct
from util.delta_storage import DeltaStorageHandler
from schema import get_schema  # your existing schema loader


# Fake fraud model: randomly assign True/False
def fake_fraud_detector():
    return random.choice([True, False])

fake_fraud_udf = udf(fake_fraud_detector, BooleanType())


def simulate_and_respond(
    handler,
    kafka_broker: str,
    topic: str,
    output_topic: str,
    datasource: str,
    dataset: str,
    starting_offsets: str = "latest"
):
    schema = get_schema(datasource, dataset)

    # Step 1: Read from Kafka
    kafka_df = handler.spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offsets) \
        .load()

    # Step 2: Parse Kafka messages
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # Step 3: Simulate fraud detection
    enriched_df = parsed_df.withColumn("is_fraud", fake_fraud_udf())

    # Step 4: Print to console
    console_query = enriched_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("append") \
        .start()

    # Step 5: Prepare to write to Kafka
    kafka_output_df = enriched_df \
        .withColumn("value", to_json(struct([col(c) for c in enriched_df.columns]))) \
        .selectExpr("CAST(value AS STRING)")

    kafka_query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("topic", output_topic) \
        .option("checkpointLocation", f"/tmp/checkpoints/fraud_response") \
        .outputMode("append") \
        .start()

    return [console_query, kafka_query]


if __name__ == "__main__":
    try:
        KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
        print(f"Using Kafka broker at: {KAFKA_BROKER}")

        handler = DeltaStorageHandler()
        queries = simulate_and_respond(
            handler=handler,
            kafka_broker=KAFKA_BROKER,
            topic="fraud_detect",
            output_topic="fraud_response",
            datasource="fraud_detect",
            dataset="fraud_kyc",
            starting_offsets="latest"
        )

        # Await termination of both streams
        for query in queries:
            query.awaitTermination()

    except Exception as e:
        print("Exception occurred during streaming:")
        traceback.print_exc()