import requests
from util.delta_storage import DeltaStorageHandler

# Define ECB API parameters
ECB_URL = "https://data-api.ecb.europa.eu/service/data/"
FLOW_REF = "EXR/D..EUR.SP00.A"
PARAMS = {
    "format": "jsondata",
    "startPeriod": "2021-02-01",
    "endPeriod": "2021-02-28",
}


def get_exchange_rates():
    response = requests.get(ECB_URL + FLOW_REF, params=PARAMS)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching exchange rates: {response.status_code}")
        return None


# Initialize DeltaStorageHandler (this will create the Spark session)
storage = DeltaStorageHandler()

# Process ECB exchange rates data
exchange_rates = get_exchange_rates()
if exchange_rates:
    # Write the ECB data to a Delta table
    storage.write_api_json(api_data=exchange_rates, datasource="ecb", dataset="exchange_rates")
    print("ECB exchange rates data written to Delta Lake.")
else:
    print("Failed to fetch ECB exchange rates.")

# Stop the Spark session when done
storage.stop_spark()
