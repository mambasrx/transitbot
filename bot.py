import os
import requests
import pandas as pd
from datetime import datetime

# GTFS Data Source
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

# GO Transit Stop Names (based on stops.txt)
ALDERSHOT_NAME = "Aldershot GO"
UNION_NAME = "Union Station"

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

def get_stop_ids():
    """Retrieve the correct stop IDs for Aldershot and Union from stops.txt."""
    stops_path = os.path.join(GTFS_DIR, "stops.txt")

    try:
        stops_df = pd.read_csv(stops_path, usecols=["stop_id", "stop_name"])
    except FileNotFoundError:
        raise Exception("❌ stops.txt not found.")

    aldershot_id = stops_df[stops_df["stop_name"].str.contains(ALDERSHOT_NAME, case=False, na=False)]["stop_id"].values
    union_id = stops_df[stops_df["stop_name"].str.contains(UNION_NAME, case=False, na=False)]["stop_id"].values

    if len(aldershot_id) == 0 or len(union_id) == 0:
        raise Exception("❌ Could not find stop IDs for Aldershot or Union in stops.txt.")

    return aldershot_id[0], union_id[0]

def parse_gtfs():
    """Extract upcoming GO Train departures between Aldershot and Union."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"])
    except FileNotFoundError:
        raise Exception("❌ stop_times.txt not found.")

    # Get actual stop IDs from stops.txt
    aldershot_id, union_id = get_stop_ids()
    print(f"✅ Aldershot Stop ID: {aldershot_id}, Union Stop ID: {union_id}")

    # Convert time columns
    stop_times_df["departure_time"] = pd.to_datetime(stop_times_df["departure_time"], format="%H:%M:%S", errors="coerce")

    # Current time in HH:MM:SS
    now = datetime.now().strftime("%H:%M:%S")

    # Filter stop times for Aldershot and Union
    aldershot_stops = stop_times_df[stop_times_df["stop_id"] == aldershot_id]
    union_stops = stop_times_df[stop_times_df["stop_id"] == union_id]

    # Merge trips to find common ones
    merged_trips = aldershot_stops.merge(union_stops, on="trip_id", suffixes=("_AL", "_UN"))

    # Ensure correct stop order
    valid_trips = merged_trips[merged_trips["stop_sequence_AL"] < merged_trips["stop_sequence_UN"]]

    # Filter future departures
    valid_trips = valid_trips[valid_trips["departure_time_AL"] >= now]

    # Sort by departure time
    valid_trips = valid_trips.sort_values("departure_time_AL")

    # Get next 3 trips Aldershot → Union
    aldershot_to_union = valid_trips.head(3)[["departure_time_AL", "departure_time_UN"]]

    # Get next 3 trips Union → Aldershot
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
