import requests
from pyspark.sql import SparkSession
from delta import *
import json

# URL ECB API for exchange rates
url = "https://eodhd.com/api/news"


params = {
    "api_token": "demo",
    "limit": 10,
    "offset": 0,
    "fmt": "json",
    "s": "AAPL.US",
    "from" : "2023-03-01",  # Example start date
    "to" : "2023-03-10"  # Example end date
    #"t":topic

}

    
def fetch_financial_news(ticker=None, topic=None, api_token="demo", start_date=None, end_date=None, limit=10, offset=0):

    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}


# Initialize Spark session with Delta Lake support
spark = (
    SparkSession.builder.appName("News API to Delta Lake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

news_data = fetch_financial_news()
print(news_data)

if news_data:
    # Convert the entire response JSON into a Spark DataFrame
    # Parallelize the entire response JSON and load it into the DataFrame
    rdd = spark.sparkContext.parallelize([json.dumps(news_data)])
    df = spark.read.json(rdd)

    # Write the DataFrame to Delta Lake in Parquet format
    df.write.format("delta").mode("append").save("/data/news_data_eodhd")

    print(f"Data written to Delta Lake at /data/news_data_eodhd")
