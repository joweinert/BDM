import requests
from pyspark.sql import SparkSession
from delta import *
import json

# URL ECB API for exchange rates
url = "https://data-api.ecb.europa.eu/service/data/"
flow_ref = "EXR/D.USD.EUR.SP00.A"

params = {
    "format": "jsondata",
    "startPeriod": "2021-01-01",
    "endPeriod": "2021-01-31",
}


def get_exchange_rates():
    response = requests.get(url + flow_ref, params=params)
    if response.status_code == 200:
        return response.json()  # Return the entire response JSON
    else:
        print(f"Error fetching exchange rates: {response.status_code}")
        return None


# Initialize Spark session with Delta Lake support
spark = (
    SparkSession.builder.appName("ECB API to Delta Lake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# Fetch the exchange rates data
exchange_rates = get_exchange_rates()

if exchange_rates:
    # Convert the entire response JSON into a Spark DataFrame
    # Parallelize the entire response JSON and load it into the DataFrame
    rdd = spark.sparkContext.parallelize([json.dumps(exchange_rates)])
    df = spark.read.json(rdd)

    # Write the DataFrame to Delta Lake in Parquet format
    df.write.format("delta").mode("append").save("/data/exchange_rates_ecb")

    print(f"Data written to Delta Lake at /data/exchange_rates_ecb")
