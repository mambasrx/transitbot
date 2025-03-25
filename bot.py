import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# GTFS Feed URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

def fetch_gtfs():
    """Download and extract GTFS data."""
    print(f"🔄 Fetching GTFS data from: {GTFS_URL}")
    
    response = requests.get(GTFS_URL)
    if response.status_code != 200:
        raise Exception("❌ Failed to download GTFS data.")
    
    with zipfile.ZipFile(BytesIO(response.content), "r") as zip_ref:
        zip_ref.extractall(GTFS_DIR)
    
    print("✅ GTFS data extracted successfully.")

def parse_gtfs():
    """Parse GTFS schedule and print next 90 minutes of departures."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, low_memory=False)
        stop_times_df["departure_time"] = pd.to_datetime(stop_times_df["departure_time"], errors="coerce")
        now = datetime.now()
        future_time = now + timedelta(minutes=90)

        # Filter departures within the next 90 minutes
        upcoming_trains = stop_times_df[
            (stop_times_df["departure_time"] >= now) & 
            (stop_times_df["departure_time"] <= future_time)
        ]

        if upcoming_trains.empty:
            print("🚆 No upcoming departures in the next
