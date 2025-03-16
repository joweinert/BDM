import time
import json
import websocket
from kafka import KafkaProducer
import os

# Kafka Configuration
KAFKA_BROKER = "kafka:9092" if os.getenv("DOCKER_ENV") else "localhost:29092"
TOPIC_NAME = "crypto_data"  # Ensure this matches your Kafka topic

print(KAFKA_BROKER)
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# WebSocket event handlers
def on_message(ws, message):
    """Handle incoming WebSocket messages and send them to Kafka."""
    data = json.loads(message)
    print(f"Sent: {data}")
    
    if "data" in data:  # Ensure we have trade data
        for trade in data["data"]:
            producer.send(TOPIC_NAME, trade)
            print(f"Sent to Kafka: {trade}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### WebSocket closed ###")

def on_open(ws):
    """Subscribe to crypto price updates."""
    print("WebSocket connection opened")
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))  # Bitcoin
    ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:ETHUSDT"}))  # Ethereum

if __name__ == "__main__":
    print(f"Starting Kafka Producer for real-time crypto data...")

    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=cv6o309r01qi7f6r8o40cv6o309r01qi7f6r8o4g",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    ws.on_open = on_open
    ws.run_forever()