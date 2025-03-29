from datetime import datetime
from dateutil.relativedelta import relativedelta
import requests
from util.delta_storage import DeltaStorageHandler
import os

API_URL = "https://api.finnhub.io/api/v1"
DEFAULT_TIMEOUT = 10
API_KEY = os.getenv("FINNHUB_API_KEY")


def get_financials_reported(
    symbol=None,
    cik=None,
    accessNumber=None,
    freq="annual",
    from_date=None,
    to_date=None,
):
    params = {
        "symbol": symbol,
        "cik": cik,
        "accessNumber": accessNumber,
        "freq": freq,
        "from": from_date,
        "to": to_date,
        "token": API_KEY,
    }
    params = {k: v for k, v in params.items() if v is not None}

    response = requests.get(
        f"{API_URL}/stock/financials-reported", params=params, timeout=DEFAULT_TIMEOUT
    )

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed: {response.status_code}, {response.text}")


storage = DeltaStorageHandler()

today = datetime.today()
from_date = (today - relativedelta(months=3)).strftime("%Y-%m-%d")
to_date = today.strftime("%Y-%m-%d")


for symbol in ["AAPL", "GOOGL", "MSFT"]:
    financials_report = get_financials_reported(
        symbol=symbol, freq="quarterly", from_date=from_date, to_date=to_date
    )
    print(financials_report["data"])

    if (
        financials_report
        and "data" in financials_report
        and financials_report["data"] != []
    ):
        storage.write_api_json(
            financials_report["data"],
            f"financials_reported_finnhub/{symbol}",
            mode="append",
        )
    else:
        print(financials_report)
        print(
            f"Failed to fetch new Finnhub Financial Report data for {symbol}. Is there?"
        )

storage.stop_spark()
