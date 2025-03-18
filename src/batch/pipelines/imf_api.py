import requests
import json
from datetime import datetime
from util.delta_storage import DeltaStorageHandler

# Base API URL
BASE_URL = "https://www.imf.org/external/datamapper/api/v1/"

def fetch_imf_data_for_us():
    """
    Fetch all available IMF indicators for the United States (US).
    
    Returns:
        dict: A dictionary containing all IMF indicators and their respective data for the US.
    """
    # Step 1: Fetch the list of all indicators
    indicators_url = BASE_URL + "indicators"
    response = requests.get(indicators_url)

    if response.status_code != 200:
        print(f"Error fetching indicator list: {response.status_code}")
        return None

    indicators = response.json()
    indicator_ids = list(indicators.keys())  # Extract all indicator IDs
    print(f"Total indicators found: {len(indicator_ids)}")

    # Step 2: Fetch data for all indicators for the United States (US)
    us_data = {}
    for indicator in indicator_ids:
        url = f"{BASE_URL}{indicator}/US"
        data_response = requests.get(url)

        if data_response.status_code == 200:
            us_data[indicator] = data_response.json()
        else:
            print(f"Error fetching data for {indicator}: {data_response.status_code}")

    return us_data


# Initialize DeltaStorageHandler (this will create the Spark session)
storage = DeltaStorageHandler()

# Process ECB exchange rates data
us_indicator_data = fetch_imf_data_for_us()
if us_indicator_data:
    # Write the ECB data to a Delta table
    storage.write_api_json(us_indicator_data, "us_indicator_data_imf" , mode="overwrite")
    print("IMF US indicators data written to Delta Lake.")
else:
    print("Failed to fetch IMF US indicators.")

# Stop the Spark session when done
storage.stop_spark()