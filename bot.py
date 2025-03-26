import os
import requests
import pandas as pd
from datetime import datetime, time

# GTFS Data Source
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

# Stop IDs
ALDERSHOT_ID = "AL"
UNION_ID = "UN"

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
    """Extract upcoming GO Train departures between Aldershot and Union."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "departure_time", "stop_id", "stop_sequence"])
    except FileNotFoundError:
        raise Exception("❌ stop_times.txt not found.")

    # Convert departure_time to just a time (HH:MM:SS)
    stop_times_df["departure_time"] = stop_times_df["departure_time"].astype(str).str.strip()
    stop_times_df["departure_time"] = stop_times_df["departure_time"].apply(lambda x: time(*map(int, x.split(":"))))

    # Current time
    now = datetime.now().time()

    # Filter for Aldershot and Union stops
    aldershot_stops = stop_times_df[stop_times_df["stop_id"] == ALDERSHOT_ID]
    union_stops = stop_times_df[stop_times_df["stop_id"] == UNION_ID]

    # Merge on trip_id to find trips that include both stops
    merged_trips = aldershot_stops.merge(union_stops, on="trip_id", suffixes=("_AL", "_UN"))

    # Ensure correct stop order (Aldershot → Union)
    valid_trips = merged_trips[merged_trips["stop_sequence_AL"] < merged_trips["stop_sequence_UN"]]

    # Keep only future departures
    valid_trips = valid_trips[valid_trips["departure_time_AL"] >= now]

    # Sort by departure time
    valid_trips = valid_trips.sort_values("departure_time_AL")

    # Get next 3 departures Aldershot → Union
    aldershot_to_union = valid_trips.head(3)[["departure_time_AL", "departure_time_UN"]]

    # Get next 3 departures Union → Aldershot
    union_to_aldershot = merged_trips[
        (merged_trips["stop_sequence_UN"] < merged_trips["stop_sequence_AL"]) &
        (merged_trips["departure_time_UN"] >= now)
    ].sort_values("departure_time_UN").head(3)[["departure_time_UN", "departure_time_AL"]]

    # Print results
    print("\n🚆 Next 3 Departures: Aldershot → Union")
    if aldershot_to_union.empty:
        print("❌ No upcoming trips found.")
    else:
        print(aldershot_to_union.rename(columns={
            "departure_time_AL": "Aldershot Departure",
            "departure_time_UN": "Union Arrival"
        }).to_string(index=False))

    print("\n🚆 Next 3 Departures: Union → Aldershot")
    if union_to_aldershot.empty:
        print("❌ No upcoming trips found.")
    else:
        print(union_to_aldershot.rename(columns={
            "departure_time_UN": "Union Departure",
            "departure_time_AL": "Aldershot Arrival"
        }).to_string(index=False))

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
