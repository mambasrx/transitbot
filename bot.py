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
    """Convert GTFS time (HH:MM:SS) into a valid datetime object."""
    today = datetime.today()

    # Split the GTFS time (HH:MM:SS) into hours, minutes, and seconds
    hours, minutes, seconds = map(int, gtfs_time.split(":"))
    
    # Handle cases where time exceeds 24 hours (e.g., 25:00:00 is the next day)
    if hours >= 24:
        hours -= 24
        today += timedelta(days=1)  # Increment day if time is past midnight

    # Return the full datetime object
    return datetime(today.year, today.month, today.day, hours, minutes, seconds)

def get_next_trips(stop_times_df, start_stop, end_stop):
    """
    Get the next 3 trips departing **after the current time**.
    :param stop_times_df: DataFrame with stop times.
    :param start_stop: Departure stop ID (AL or UN).
    :param end_stop: Arrival stop ID (UN or AL).
    :return: List of tuples containing departure and arrival times.
    """
    current_time = datetime.now()  # Get system's current time

    # Filter only trips containing both start and end stop
    relevant_trips = stop_times_df[stop_times_df["stop_id"].isin([start_stop, end_stop])]

    # Group trips by trip_id
    trips_dict = {}
    
    for trip_id, group in relevant_trips.groupby("trip_id"):
        group = group.sort_values("stop_sequence")

        # Ensure trip contains both start and end stops
        start_row = group[group["stop_id"] == start_stop]
        end_row = group[group["stop_id"] == end_stop]

        if not start_row.empty and not end_row.empty:
            departure_time = fix_time_format(start_row["departure_time"].iloc[0])
            arrival_time = fix_time_format(end_row["departure_time"].iloc[0])

            # Only add trips that **depart after the current time**
            if departure_time > current_time:
                trips_dict[trip_id] = (departure_time, arrival_time)

    # Sort trips by departure time and get the next 3
    sorted_trips = sorted(trips_dict.values(), key=lambda x: x[0])[:3]

    return sorted_trips

def parse_gtfs():
    """Extract upcoming GO Train departures between Aldershot and Union."""
    stop_times_path = os.path.join(GTFS_FOLDER, "stop_times.txt")

    try:
        stop_times_df = pd.read_csv(stop_times_path, usecols=["trip_id", "departure_time", "stop_id", "stop_sequence"])
    except FileNotFoundError:
        print("❌ stop_times.txt not found.")
        return
    
    # Read trips file to filter only valid train trips
    trips_path = os.path.join(GTFS_FOLDER, "trips.txt")
    trips_df = pd.read_csv(trips_path, usecols=["trip_id", "route_id"])

    # Merge stop_times with trips to get route_id
    stop_times_df = stop_times_df.merge(trips_df, on="trip_id")

    # Get next 3 trips for each direction
    al_to_un_trips = get_next_trips(stop_times_df, ALDERSHOT_STOP_ID, UNION_STOP_ID)
    un_to_al_trips = get_next_trips(stop_times_df, UNION_STOP_ID, ALDERSHOT_STOP_ID)

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
