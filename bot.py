import os
import requests
import zipfile
import yaml
import pandas as pd
from mastodon import Mastodon
from datetime import datetime, timedelta
import pytz

# Load configuration
with open("config.yaml", "r") as config_file:
    config = yaml.safe_load(config_file)

# Mastodon API setup
mastodon = Mastodon(
    access_token=config["mastodon"]["access_token"],
    api_base_url=config["mastodon"]["api_base_url"]
)

# GTFS static data file location
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"

def fetch_gtfs():
    """Download and extract GTFS data."""
    zip_path = "go_gtfs.zip"
    extract_path = "gtfs_data"

    print("🔄 Fetching GTFS data from:", GTFS_URL)

    try:
        response = requests.get(GTFS_URL, stream=True)
        if response.status_code == 200:
            with open(zip_path, "wb") as f:
                f.write(response.content)
            print("✅ GTFS data downloaded successfully.")

            # Extract the ZIP file
            os.makedirs(extract_path, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)
            print("✅ GTFS data extracted successfully.")
        else:
            print(f"❌ HTTP Error: {response.status_code}")
            print("⚠️ Full response text:", response.text)
            raise Exception("Failed to download GTFS data.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        raise Exception("Network error while downloading GTFS data.")

def parse_gtfs():
    """Parse GTFS schedule data for the next 90 minutes."""
    fetch_gtfs()  # Ensure GTFS is downloaded & extracted
    
    stop_times_path = "gtfs_data/stop_times.txt"
    if not os.path.exists(stop_times_path):
        raise FileNotFoundError(f"❌ GTFS file not found: {stop_times_path}")

    # Load GTFS stop_times.txt
    stop_times_df = pd.read_csv(stop_times_path)
    print("✅ Successfully loaded GTFS stop_times.txt")

    # Convert departure_time to datetime
    now = datetime.now(pytz.timezone("America/Toronto"))
    time_window = now + timedelta(minutes=90)
    
    stop_times_df["departure_time"] = pd.to_datetime(stop_times_df["departure_time"], errors="coerce")
    upcoming_trains = stop_times_df[
        (stop_times_df["departure_time"] >= now) & 
        (stop_times_df["departure_time"] <= time_window)
    ]

    if upcoming_trains.empty:
        return "No upcoming departures in the next 90 minutes."
    
    return upcoming_trains[["trip_id", "departure_time"]].to_string(index=False)

def post_to_mastodon():
    """Post train schedule updates to Mastodon."""
    train_schedule = parse_gtfs()
    message = f"🚆 Upcoming GO Train Departures (Next 90 min):\n\n{train_schedule}"
    
    mastodon.status_post(message)
    print("✅ Posted to Mastodon:", message)

if __name__ == "__main__":
    post_to_mastodon()
