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
    """Convert GTFS time format to a valid datetime.time object."""
    try:
        hours, minutes, seconds = map(int, gtfs_time.split(":"))
        if hours >= 24:
            hours -= 24  # Adjust for GTFS extended hour format
        return time(hours, minutes, seconds)
    except ValueError:
        return None  # Handle potential bad data gracefully

def get_upcoming_trips(stop_times_df, direction):
    """
    Extracts the next 3 train departures **from the current time** for a given direction.
    :param stop_times_df: DataFrame with stop times.
    :param direction: "AL-UN" for Aldershot → Union, "UN-AL" for Union → Aldershot.
    :return: List of tuples containing departure and arrival times.
    """
    current_time = datetime.now().time()  # Get the current system time

    if direction == "AL-UN":
        start_stop = ALDERSHOT_STOP_ID
        end_stop = UNION_STOP_ID
    else:
        start_stop = UNION_STOP_ID
        end_stop = ALDERSHOT_STOP_ID

    # Filter trips that contain both the start and end stop
    relevant_trips = stop_times_df[stop_times_df["stop_id"].isin([start_stop, end_stop])]

    # Group trips by trip_id
    trip_groups = relevant_trips.groupby("trip_id")
    valid_trips = []

    for trip_id, group in trip_groups:
        group = group.sort_values("stop_sequence")

        # Ensure the trip starts at the correct station and ends at the destination
        if group.iloc[0]["stop_id"] == start_stop and group.iloc[-1]["stop_id"] == end_stop:
            departure_time = group.iloc[0]["departure_time"]
            arrival_time = group.iloc[-1]["departure_time"]

            # Only consider upcoming trips
            if departure_time > current_time:
                valid_trips.append((departure_time, arrival_time))

    # Sort by departure time and return the next 3 trips
    valid_trips.sort()
    return valid_trips[:3]

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

    # Get next 3 trips for each direction
    al_to_un_trips = get_upcoming_trips(stop_times_df, "AL-UN")
    un_to_al_trips = get_upcoming_trips(stop_times_df, "UN-AL")

    # Print the results
    print("\n🚆 Next 3 Departures: Aldershot → Union")
    print(f"{'Aldershot Departure':<20} {'Union Arrival':<20}")
    for dep, arr in al_to_un_trips:
        print(f"{dep} {arr}")

    print("\n🚆 Next 3 Departures: Union → Aldershot")
    print(f"{'Union Departure':<20} {'Aldershot Arrival':<20}")
    for dep, arr in un_to_al_trips:
        print(f"{dep} {arr}")

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
