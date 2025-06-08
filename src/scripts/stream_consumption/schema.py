from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType,TimestampType

def get_schema(datasource: str, dataset: str) -> StructType:
        """
        Return the Spark StructType schema depending on datasource or dataset.
        """
        if datasource == "crypto_stream":
            return StructType([
                StructField("c", StringType(), True),
                StructField("p", DoubleType(), True),
                StructField("s", StringType(), True),
                StructField("t", LongType(), True),
                StructField("v", DoubleType(), True)
            ])
    
        elif datasource == "fraud_detect":
            return StructType([
                StructField("event", StringType()),
                StructField("request_id", StringType()),
                StructField("user_id", StringType()),
                StructField("doc_s3_path", StringType()),
                StructField("face_s3_path", StringType()),
                StructField("upload_time", StringType()),
                StructField("status", StringType()),
])

        elif datasource == "stock_stream":  # New schema for your stock data
            return StructType([
                StructField("symbol", StringType(), True),
                StructField("timestamp", TimestampType(), True),  # timestamp string -> TimestampType
                StructField("current_price", DoubleType(), True),
                StructField("open_price", DoubleType(), True),
                StructField("day_high", DoubleType(), True),
                StructField("day_low", DoubleType(), True),
                StructField("volume", LongType(), True),  # volume is integer/long
            ])
    
        else:
            raise ValueError(f"Unknown datasource {datasource}")
