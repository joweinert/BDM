import requests
from util.delta_storage import DeltaStorageHandler
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

def fetch_financial_news():

    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}


# Initialize DeltaStorageHandler (this will create the Spark session)
storage = DeltaStorageHandler()

# Process ECB exchange rates data
financial_news = fetch_financial_news()
if financial_news:
    # Write the ECB data to a Delta table
    storage.write_api_json(financial_news, "financial_news_eodhd", mode="overwrite")
    print("EOFHD financial news data written to Delta Lake.")
else:
    print("Failed to fetch EOFHD financial news.")

# Stop the Spark session when done
storage.stop_spark()