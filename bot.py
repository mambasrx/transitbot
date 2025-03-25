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

# GTFS static data file location (update if needed)
GTFS_URL = "https://www.gotransit.com/static_files/gtfs/GO_GTFS.zip"

def fetch_gtfs():
    """Download and extract GTFS data."""
    zip_path = "go_gtfs.zip"
    extract_path = "gtfs_data"

    # Download GTFS file
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
        raise Exception("❌ Failed to download GTFS data.")

def parse_gtfs():
    """Parse GTFS schedule data for the next 90 minutes."""
    fetch_gtfs()  # Ensure GTFS is downloaded &_
