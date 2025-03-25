import requests
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

# GTFS static data file location (replace with real data source)
GTFS_URL = "https://www.gotransit.com/static_files/gtfs/GO_GTFS.zip"

def fetch_gtfs():
    """Download and extract GTFS data."""
    response = requests.get(GTFS_URL)
    with open("go_gtfs.zip", "wb") as f:
        f.write(response.content)

def parse_gtfs():
    """Parse GTFS schedule data for the next 90 minutes."""
    fetch_gtfs()
    gtfs_data = pd.read_csv("gtfs_data/stop_times.txt")  # Example GTFS parsing
    
    # Time filtering
    now = datetime.now(pytz.timezone("America/Toronto"))
    time_window = now + timedelta(minutes=90)
    
    gtfs_data["departure_time"] = pd.to_datetime(gtfs_data["departure_time"])
    upcoming_trains = gtfs_data[(gtfs_data["departure_time"] >= now) & (gtfs_data["departure_time"] <= time_window)]

    return upcoming_trains[["trip_id", "departure_time"]].to_string(index=False)

def post_to_mastodon():
    """Post train schedule updates to Mastodon."""
    train_schedule = parse_gtfs()
    message = f"🚆 Upcoming GO Train Departures (Next 90 min):\n\n{train_schedule}"
    
    mastodon.status_post(message)
    print("Posted to Mastodon:", message)

if __name__ == "__main__":
    post_to_mastodon()
