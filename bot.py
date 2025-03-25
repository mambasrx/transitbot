import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# GTFS data source
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

# Stop IDs for Aldershot and Union
ALDERSHOT_STOP_ID = "AL"
UNION_STOP_ID = "UN"

# Ensure GTFS directory exists
os.makedirs(GTFS_DIR, exist_ok=True)

def fetch_gtfs():
    """Download and extract the latest GTFS data."""
    zip_path = os.path.join(GTFS_DIR, "gtfs.zip")

    print(f"🔄 Fetching GTFS data from: {GTFS_URL}")
    response = requests.get(GTFS_URL)
    if response.status_code == 200:
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print("✅ GTFS data downloaded successfully.")
        os.system(f"unzip -o {zip_path} -d {GTFS_DIR}")
        print("✅ GTFS data extracted successfully.")
    else:
        raise Exception("❌ Failed to download GTFS data.")

def parse_gtfs():
    """Parse GTFS stop_times and trips data to get next departures."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
    trips_path = os.path.join(GTFS_DIR, "trips.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "arrival_time", "departure_time", "stop_id"])
        trips_df = pd.read_csv(trips_path, usecols=["trip_id", "route_id"])
    except FileNotFoundError:
        raise Exception("❌ GTFS files not found. Ensure data is downloaded correctly.")

    # Merge trip details
    merged_df = stop_times_df.merge(trips_df, on="trip_id")

    # Get current time in HH:MM:SS format
    now = datetime.now().strftime("%H:%M:%S")
    next_90_min = (datetime.now() + timedelta(minutes=90)).strftime("%H:%M:%S")

    # Filter for Aldershot (AL) → Union (UN) and Union (UN) → Aldershot (AL)
    departures = merged_df[
        ((merged_df["stop_id"] == ALDERSHOT_STOP_ID) & (merged_df["departure_time"] >= now) & (merged_df["departure_time"] <= next_90_min)) |
        ((merged_df["stop_id"] == UNION_STOP_ID) & (merged_df["departure_time"] >= now) & (merged_df["departure_time"] <= next_90_min))
    ]

    # Remove trip_id and date from departure_time
    departures["departure_time"] = departures["departure_time"].str.split(" ").str[-1]

    if departures.empty:
        print("🚆 No upcoming departures in the next 90 minutes.")
    else:
        print("\n🚆 Upcoming Departures (Next 90 Minutes):")
        print(departures[["stop_id", "departure_time", "route_id"]].to_string(index=False))

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
