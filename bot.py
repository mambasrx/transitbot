import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# GTFS Source URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_FOLDER = "gtfs_data"

# Stop IDs for Aldershot and Union
ALDERSHOT_STOP_ID = "AL"
UNION_STOP_ID = "UN"

def fetch_gtfs():
    """Fetch and extract the latest GTFS data."""
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
    """Convert GTFS time (which can be over 24:00:00) into a valid datetime object."""
    today = datetime.today()

    # Split the GTFS time (HH:MM:SS) into hours, minutes, and seconds
    hours, minutes, seconds = map(int, gtfs_time.split(":"))
    
    # Handle cases where time exceeds 24 hours (e.g., 25:00:00 is the next day)
    if hours >= 24:
        hours -= 24
        today += timedelta(days=1)  # Increment day if time is past midnight

    # Return the full datetime object
    return datetime(today.year, today.month, today.day, hours, minutes, seconds)

def get_upcoming_trips(stop_times_df, start_stop, end_stop):
    """
    Extracts the next 3 train departures from **the current time**.
    :param stop_times_df: DataFrame with stop times.
    :param start_stop: Starting stop ID (AL or UN).
    :param end_stop: Ending stop ID (UN or AL).
    :return: List of tuples containing departure and arrival times.
    """
    current_time = datetime.now()  # Get current system time

    # Get trips that contain both the start and end stop
    relevant_trips = stop_times_df[stop_times_df["stop_id"].isin([start_stop, end_stop])]

    # Group trips by trip_id
    trip_groups = relevant_trips.groupby("trip_id")
    valid_trips = []

    for trip_id, group in trip_groups:
        group = group.sort_values("stop_sequence")

        # Ensure the trip starts at the correct station and ends at the destination
        start_row = group[group["stop_id"] == start_stop]
        end_row = group[group["stop_id"] == end_stop]

        if not start_row.empty and not end_row.empty:
            departure_time = start_row["departure_time"].values[0]
            arrival_time = end_row["departure_time"].values[0]

            # Only consider **upcoming** trips
            if departure_time > current_time:
                valid_trips.append((departure_time, arrival_time, trip_id))  # Include trip_id to ensure uniqueness

    # Sort by departure time and return the next 3 **distinct** trips
    valid_trips.sort()
    unique_trips = []
    seen_trip_ids = set()

    for dep, arr, trip_id in valid_trips:
        if trip_id not in seen_trip_ids:
            unique_trips.append((dep, arr))
            seen_trip_ids.add(trip_id)
        if len(unique_trips) == 3:
            break  # Stop after getting 3 unique trips

    return unique_trips

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
    al_to_un_trips = get_upcoming_trips(stop_times_df, ALDERSHOT_STOP_ID, UNION_STOP_ID)
    un_to_al_trips = get_upcoming_trips(stop_times_df, UNION_STOP_ID, ALDERSHOT_STOP_ID)

    # Print the results
    print("\n🚆 Next 3 Departures: Aldershot → Union")
    print(f"{'Aldershot Departure':<20} {'Union Arrival':<20}")
    for dep, arr in al_to_un_trips:
        print(f"{dep.strftime('%H:%M:%S')} {arr.strftime('%H:%M:%S')}")

    print("\n🚆 Next 3 Departures: Union → Aldershot")
    print(f"{'Union Departure':<20} {'Aldershot Arrival':<20}")
    for dep, arr in un_to_al_trips:
        print(f"{dep.strftime('%H:%M:%S')} {arr.strftime('%H:%M:%S')}")

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
