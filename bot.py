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
    """Parse GTFS data and filter for next 3 trips from Aldershot to Union and vice versa."""
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "arrival_time", "departure_time", "stop_id"])
    except FileNotFoundError:
        raise Exception("❌ GTFS files not found. Ensure data is downloaded correctly.")

    # Get current time in HH:MM:SS format
    now = datetime.now().strftime("%H:%M:%S")

    # Convert times to datetime for sorting/filtering
    stop_times_df["departure_time"] = pd.to_datetime(stop_times_df["departure_time"], format="%H:%M:%S", errors="coerce")

    # Filter only today's departures
    stop_times_df = stop_times_df[stop_times_df["departure_time"].notna()]
    stop_times_df = stop_times_df[stop_times_df["departure_time"] >= now]

    # Get trips departing from Aldershot (AL)
    aldershot_departures = stop_times_df[stop_times_df["stop_id"] == ALDERSHOT_STOP_ID].sort_values("departure_time")

    # Get trips that arrive at Union (UN) for those Aldershot departures
    aldershot_to_union = aldershot_departures.merge(stop_times_df, on="trip_id", suffixes=("_AL", "_UN"))
    aldershot_to_union = aldershot_to_union[
        (aldershot_to_union["stop_id_UN"] == UNION_STOP_ID) &
        (aldershot_to_union["departure_time_UN"] > aldershot_to_union["departure_time_AL"])
    ].head(3)

    # Get trips departing from Union (UN)
    union_departures = stop_times_df[stop_times_df["stop_id"] == UNION_STOP_ID].sort_values("departure_time")

    # Get trips that stop at Aldershot (AL) for those Union departures
    union_to_aldershot = union_departures.merge(stop_times_df, on="trip_id", suffixes=("_UN", "_AL"))
    union_to_aldershot = union_to_aldershot[
        (union_to_aldershot["stop_id_AL"] == ALDERSHOT_STOP_ID) &
        (union_to_aldershot["departure_time_AL"] > union_to_aldershot["departure_time_UN"])
    ].head(3)

    print("\n🚆 Next 3 Departures: Aldershot → Union")
    if aldershot_to_union.empty:
        print("❌ No upcoming trips found.")
    else:
        print(aldershot_to_union[["departure_time_AL", "departure_time_UN"]].rename(columns={
            "departure_time_AL": "Aldershot Departure",
            "departure_time_UN": "Union Arrival"
        }).to_string(index=False))

    print("\n🚆 Next 3 Departures: Union → Aldershot")
    if union_to_aldershot.empty:
        print("❌ No upcoming trips found.")
    else:
        print(union_to_aldershot[["departure_time_UN", "departure_time_AL"]].rename(columns={
            "departure_time_UN": "Union Departure",
            "departure_time_AL": "Aldershot Arrival"
        }).to_string(index=False))

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
