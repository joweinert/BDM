import yfinance as yf
import json
import time
from kafka import KafkaProducer
import os

# Kafka Configuration
KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
TOPIC_NAME = "stock_data"  # Ensure this matches your Kafka topic

print(KAFKA_BROKER)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Stock symbol you want to track
STOCK_SYMBOL = "AAPL"


def fetch_live_stock_price():
    """Fetch the real-time stock price using Yahoo Finance."""
    # try:
    ticker = yf.Ticker(STOCK_SYMBOL)
    stock_info = ticker.info  # More reliable than fast_info
    # Ensure data exists before sending to Kafka
    if "regularMarketPrice" not in stock_info:
        print(f"⚠ No real-time price data available for {STOCK_SYMBOL}.")
        return None
    return {
        "symbol": STOCK_SYMBOL,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "current_price": float(stock_info["regularMarketPrice"]),
        "open_price": float(stock_info.get("open", 0)),  # Default to 0 if missing
        "day_high": float(stock_info.get("dayHigh", 0)),
        "day_low": float(stock_info.get("dayLow", 0)),
        "volume": int(stock_info.get("volume", 0)),
    }

    # except Exception as e:


#     print(f"❌ Error fetching stock price: {e}")
#    return None


if __name__ == "__main__":
    print(f"Starting Kafka Producer for real-time {STOCK_SYMBOL} stock data...")

    while True:
        stock_data = fetch_live_stock_price()
        if stock_data:
            producer.send(TOPIC_NAME, stock_data)
            print(f"Sent: {stock_data}")
        time.sleep(1)
