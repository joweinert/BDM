import requests, os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from time import sleep
from util.delta_storage import DeltaStorageHandler

BIG_DATA = os.getenv("BIGDATA_MODE", "false").lower() == "true"
BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"
COUNTRY_CODES = ["USA", "ESP"]

retry_strategy = Retry(
    total=3,  # retrys
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
    backoff_factor=1,
)
adapter = HTTPAdapter(max_retries=retry_strategy)

session = requests.Session()
session.mount("http://", adapter)
session.mount("https://", adapter)


def filter_new_years(time_series: dict, latest_year: int) -> dict:
    return {
        year: value for year, value in time_series.items() if int(year) > latest_year
    }


def fetch_indicator_data(country, indicator: str):
    if not indicator.strip():
        print("⚠️ Skipping empty indicator key")
        return None
    url = f"{BASE_URL}{indicator}/{country}"
    data_response = session.get(url)
    if data_response.status_code != 200:
        print(f"⚠️ Failed to fetch {indicator}: {data_response.status_code}")
        return None
    values = data_response.json().get("values", {})
    country_data = values.get(indicator, {}).get(country, {})
    if not country_data:
        print(f"⚠️ No data found for {indicator} in {country}")
        return None
    return country_data


if __name__ == "__main__":
    storage = DeltaStorageHandler()

    indicators_url = BASE_URL + "indicators"
    response = session.get(indicators_url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch indicators: {response.status_code}")
        exit()

    indicator_ids = list(response.json().get("indicators", {}).keys())
    if not BIG_DATA:
        indicator_ids = indicator_ids[:10]
    for country in COUNTRY_CODES:
        for indicator in indicator_ids:
            data = fetch_indicator_data(country=country, indicator=indicator)
            if data is None:
                continue
            table_name = storage.write_api_json(
                api_data=data,
                datasource="imf",
                dataset=f"{country}_{indicator}",
                mode="append",
            )
            print(f"✅ Wrote {indicator} for {country} to {table_name}")
            sleep(1)

    storage.stop_spark()
