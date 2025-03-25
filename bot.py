import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# GTFS Feed URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

# Define Stop IDs for Aldershot and Union (these come from GTFS 'stops.txt')
ALDERSHOT_STOP_ID = "ALDERSHOT"
UNION_STOP_ID = "UNION"

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
    """Parse GTFS schedule and print next 90 minutes of Aldershot-Union departures."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
    trips_path = os.path.join(GTFS_DIR, "trips.txt")

    try:
        # Load GTFS stop_times.txt and trips.txt
        stop_times_df = pd.read_csv(stop_times_path, low_memory=False)
        trips_df = pd.read_csv(trips_path, low_memory=False)

        # Merge stop_times with trip info
        merged_df = stop_times_df.merge(trips_df, on="trip_id")

        # Convert departure_time to datetime
        merged_df["departure_time"] = pd.to_datetime(merged_df["departure_time"], errors="coerce")
        now = datetime.now()
        future_time = now + timedelta(minutes=90)

        # Filter for departures within the next 90 minutes
        upcoming_trains = merged_df[
            (merged_df["departure_time"] >= now) & 
            (merged_df["departure_time"] <= future_time) &
            (merged_df["stop_id"].isin([ALDERSHOT_STOP_ID, UNION_STOP_ID]))  # Filter for relevant stations
        ]

        # Ensure the train is moving in the correct direction
        ald_to_union = upcoming_trains[upcoming_trains["stop_id"] == ALDERSHOT_STOP_ID]
        union_to_ald = upcoming_trains[upcoming_trains["stop_id"] == UNION_STOP_ID]

        if ald_to_union.empty and union_to_ald.empty:
            print("🚆 No upcoming Aldershot-Union or Union-Aldershot departures in the next 90 minutes.")
        else:
            print("🚆 Upcoming Train Departures:")
            
            if not ald_to_union.empty:
                ald_to_union["departure_time"] = ald_to_union["departure_time"].dt.strftime("%H:%M")
                print("🟢 Aldershot → Union")
                print(ald_to_union[["departure_time"]].to_string(index=False))
            
            if not union_to_ald.empty:
                union_to_ald["departure_time"] = union_to_ald["departure_time"].dt.strftime("%H:%M")
                print("🔵 Union → Aldershot")
                print(union_to_ald[["departure_time"]].to_string(index=False))

    except Exception as e:
        print(f"❌ Error parsing GTFS data: {e}")

if __name__ == "__main__":
    fetch_gtfs()
    parse_gtfs()
