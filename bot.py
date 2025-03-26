import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, time

# GTFS Source URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_FOLDER = "gtfs_data"

# Stop IDs for Aldershot and Union
ALDERSHOT_STOP_ID = "AL"
UNION_STOP_ID = "UN"

def fetch_gtfs():
    """Fetch the latest GTFS data and extract it."""
    os.makedirs(GTFS_FOLDER, exist_ok=True)
    zip_path = os.path.join(GTFS_FOLDER, "gtfs.zip")

    print(f"🔄 Fetching GTFS data from: {GTFS_URL}")
    response = requests.get(GTFS_URL)
    
    if response.status_code == 200:
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print("✅ GTFS data downloaded successfully.")
    else:
        print(f"❌ Failed to download GTFS data. HTTP {response.status_code}")
        return

    # Extract GTFS files
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(GTFS_FOLDER)
    print("✅ GTFS data extracted successfully.")

def fix_time_format(gtfs_time):
    """Convert GTFS time format (which may include 24:xx:xx) to valid 0-23 hour time."""
    try:
        hours, minutes, seconds = map(int, gtfs_time.split(":"))
        if hours >= 24:
            hours -= 24
        return time(hours, minutes, seconds)
    except ValueError:
        return None  # Handle potential bad data gracefully

def parse_gtfs():
    """Extract upcoming GO Train departures between Aldershot and Union."""
    stop_times_path = os.path.join(GTFS_FOLDER, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "departure_time", "stop_id", "stop_sequence"])
    except FileNotFoundError:
        print("❌ stop_times.txt not found.")
        return
    
    # Fix GTFS time formatting
    stop_times_df["departure_time"] = stop_times_df["departure_time"].astype(str).str.strip()
    stop_times_df["departure_time"] = stop_times_df["departure_time"].apply(fix_time_format)
    stop_times_df = stop_times_df.dropna(subset=["departure_time"])  # Remove invalid times

    # Read trips file to filter only valid train trips
    trips_path = os.path.join(GTFS_FOLDER, "trips.txt")
    trips_df = pd.read_csv(trips_path, usecols=["trip_id", "route_id"])

    # Merge stop_times with trips to get route_id
    stop_times_df = stop_times_df.merge(trips_df, on="trip_id")

    # Filter for trains that travel **Aldershot → Union**
    al_to_un_trips = stop_times_df[
        (stop_times_df["stop_id"] == ALDERSHOT_STOP_ID) |
        (stop_times_df["stop_id"] == UNION_STOP_ID)
    ].sort_values(["trip_id", "stop_sequence"])

    al_to_un_trips = al_to_un_trips.groupby("trip_id").filter(
        lambda g: (ALDERSHOT_STOP_ID in g["stop_id"].values) and (UNION_STOP_ID in g["stop_id"].values)
    )

    # Extract departure times
    departures = []
    for trip_id, group in al_to_un_trips.groupby("trip_id"):
        group = group.sort_values("stop_sequence")
        if group.iloc[0]["stop_id"] == ALDERSHOT_STOP_ID and group.iloc[-1]["stop_id"] == UNION_STOP_ID:
            departures.append((group.iloc[0]["departure_time"], group.iloc[-1]["departure_time"]))
    
    # Show next 3 trips
    print("\n🚆 Next 3 Departures: Aldershot → Union")
    print(f"{'Aldershot Departure':<20} {'Union Arrival':<20}")
    for dep, arr in departures[:3]:
        print(f"{dep} {arr}")

    # Filter for trains that travel **Union → Aldershot**
    un_to_al_trips = stop_times_df[
        (stop_times_df["stop_id"] == UNION_STOP_ID) |
        (stop_times_df["stop_id"] == ALDERSHOT_STOP_ID)
    ].sort_values(["trip_id", "stop_sequence"])

    un_to_al_trips = un_to_al_trips.groupby("trip_id").filter(
        lambda g: (UNION_STOP_ID in g["stop_id"].values) and (ALDERSHOT_STOP_ID in g["stop_id"].values)
    )

    # Extract return trips
    return_trips = []
    for trip_id, group in un_to_al_trips.groupby("trip_id"):
        group = group.sort_values("stop_sequence")
        if group.iloc[0]["stop_id"] == UNION_STOP_ID and group.iloc[-1]["stop_id"] == ALDERSHOT_STOP_ID:
            return_trips.append((group.iloc[0]["departure_time"], group.iloc[-1]["departure_time"]))

    # Show next 3 return trips
    print("\n🚆 Next 3 Departures: Union → Aldershot")
    print(f"{'Union Departure':<20} {'Aldershot Arrival':<20}")
    for dep, arr in return_trips[:3]:
        print(f"{dep} {arr}")

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
