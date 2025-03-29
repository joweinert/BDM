import requests, os
from time import sleep
from util.delta_storage import DeltaStorageHandler
from datetime import datetime

DEV = os.getenv("DEV", "false").lower() == "true"
BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"
COUNTRY_CODE = "USA"


def filter_new_years(time_series: dict, latest_year: int) -> dict:
    return {
        year: value for year, value in time_series.items() if int(year) > latest_year
    }


if __name__ == "__main__":
    storage = DeltaStorageHandler()

    indicators_url = BASE_URL + "indicators"
    response = requests.get(indicators_url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch indicators: {response.status_code}")
        exit()

    indicator_ids = list(response.json().get("indicators", {}).keys())
    if DEV:
        indicator_ids = indicator_ids[:10]

    for indicator in indicator_ids:
        if not indicator.strip():
            print("⚠️ Skipping empty indicator key")
            continue

        url = f"{BASE_URL}{indicator}/{COUNTRY_CODE}"
        data_response = requests.get(url)
        if data_response.status_code != 200:
            print(f"⚠️ Failed to fetch {indicator}: {data_response.status_code}")
            continue

        values = data_response.json().get("values", {})
        country_data = values.get(indicator, {}).get(COUNTRY_CODE, {})

        if not country_data:
            print(f"⚠️ No data found for {indicator} in {COUNTRY_CODE}")
            continue

        table_name = f"imf/imf_{indicator}"
        latest_year = (
            storage.get_max_column_value(table_name=table_name, column="year") or 0
        )
        new_data = filter_new_years(country_data, latest_year)

        if not new_data:
            print(f"⏭️ No new data for {indicator}")
            continue

        records = [
            {
                "indicator": indicator,
                "year": int(year),
                "value": value,
                "country": COUNTRY_CODE,
                "fetched_at": datetime.utcnow().isoformat(),
            }
            for year, value in new_data.items()
        ]

        storage.write_api_json(records, table_name=table_name, mode="append")
        print(f"✅ Wrote {len(records)} new records to {table_name}")
        sleep(1)

    storage.stop_spark()
