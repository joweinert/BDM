import requests
from pyspark.sql import SparkSession
from delta import *
import json
from datetime import datetime

# Base API URL
BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"

# Step 1: Fetch the list of all indicators
indicators_url = BASE_URL + "indicators"
response = requests.get(indicators_url)

if response.status_code == 200:
    indicators = response.json()
    indicator_ids = list(indicators.keys())  # Extract all indicator IDs
    print(f"Total indicators found: {len(indicator_ids)}")

    # Step 2: Fetch data for all indicators for the United States (US)
    us_data = {}
    for indicator in indicator_ids:
        url = f"{BASE_URL}{indicator}/US"
        data_response = requests.get(url)

        if data_response.status_code == 200:
            us_data[indicator] = data_response.json()

    # Print the retrieved data
    print(json.dumps(us_data, indent=4))

else:
    print(f"Error fetching indicator list: {response.status_code}")

"""
# Initialize Spark session with Delta Lake support
spark = (
    SparkSession.builder.appName("IMF Data to Delta Lake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# Fetch the IMF data
imf_data = get_imf_data()

if imf_data:
    # Convert the entire response JSON into a Spark DataFrame
    # Here, we parallelize the entire response JSON and load it into the DataFrame
    rdd = spark.sparkContext.parallelize([json.dumps(imf_data)])
    df = spark.read.json(rdd)

    # Write the DataFrame to Delta Lake in Parquet format
    df.write.format("delta").mode("append").save("/data/imf_data_current")

    print(f"Data written to Delta Lake at /data/imf_data_current")
"""
