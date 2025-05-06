import requests
from util.delta_storage import DeltaStorageHandler

url = "https://eodhd.com/api/news"


params = {
    "api_token": "demo",
    "limit": 10,
    "offset": 0,
    "fmt": "json",
    "s": "AAPL.US",
    "from": "2023-03-01",
    "to": "2023-03-10",
    # "t":topic
}


def fetch_financial_news():

    response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Failed to fetch data. Status code: {response.status_code}"}


storage = DeltaStorageHandler()

financial_news = fetch_financial_news()
if financial_news:
    print(financial_news)
    storage.write_api_json(
        api_data=financial_news, datasource="eodhd", dataset="financial_news"
    )
    print("EODHD financial news data written to Delta Lake.")
else:
    print("Failed to fetch EODHD financial news.")

storage.stop_spark()
