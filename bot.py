import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from mastodon import Mastodon

# Load credentials from GitHub Secrets (set in Actions environment)
MASTODON_ACCESS_TOKEN = os.getenv("MASTODON_ACCESS_TOKEN")
MASTODON_API_BASE_URL = os.getenv("MASTODON_API_BASE_URL")

GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_FOLDER = "gtfs_data"

# Ensure API credentials exist
if not MASTODON_ACCESS_TOKEN or not MASTODON_API_BASE_URL:
    raise Exception("❌ Mastodon API credentials are missing! Ensure secrets are set in GitHub Actions.")

def fetch_gtfs():
    """Downloads and extracts GTFS data."""
    print(f"🔄 Fetching GTFS data from: {GTFS_URL}")
    response = requests.get(GTFS_URL)
    if response.status_code != 200:
        raise Exception("❌ Failed to download GTFS data.")

    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
        zip_ref.extractall(GTFS_FOLDER)
    
    print("✅ GTFS data extracted successfully.")

def parse_gtfs():
    """Parses GTFS data and retrieves upcoming departures within 90 minutes."""
    stop_times_path = os.path.join(GTFS_FOLDER, "stop_times.txt")
    if not os.path.exists(stop_times_path):
        raise FileNotFoundError("❌ stop_times.txt not found. Ensure GTFS data is downloaded.")

    stop_times_df = pd.read_csv(stop_times_path, dtype=str)
    stop_times_df["departure_time"] = pd.to_datetime(stop_times_df["departure_time"], errors="coerce")

    now = datetime.now()
    next_90_min = now + timedelta(minutes=90)

    # Convert timezone-naive datetime to string for comparison
    filtered_trains = stop_times_df[
        (stop_times_df["departure_time"] >= now.strftime('%H:%M:%S')) &
        (stop_times_df["departure_time"] <= next_90_min.strftime('%H:%M:%S'))
    ]

    return filtered_trains

def post_to_mastodon():
    """Posts train schedule updates to Mastodon."""
    mastodon = Mastodon(
        access_token=MASTODON_ACCESS_TOKEN,
        api_base_url=MASTODON_API_BASE_URL
    )

    fetch_gtfs()
    train_schedule = parse_gtfs()

    if train_schedule.empty:
        message = "🚆 No upcoming GO Train departures in the next 90 minutes."
    else:
        message = "🚆 Upcoming GO Train departures in the next 90 minutes:\n"
        for _, row in train_schedule.iterrows():
            message += f"➡️ Train at {row['departure_time']} from stop {row['stop_id']}\n"

    print("✅ Posting to Mastodon...")
    mastodon.status_post(message)

if __name__ == "__main__":
    post_to_mastodon()
