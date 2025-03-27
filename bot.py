import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta

# GTFS Source URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_FOLDER = "gtfs_data"

# Stop IDs for Aldershot and Union (these must match GTFS data)
ALDERSHOT_STOP_ID = "ALDERSHOT"
UNION_STOP_ID = "UNION"

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

def convert_gtfs_time(gtfs_time, trip_date):
    """
    Convert GTFS time (HH:MM:SS) and trip date into a proper datetime object.
    GTFS sometimes has hours over 24, which means the trip extends past midnight.
    """
    hours, minutes, seconds = map(int, gtfs_time.split(":"))
    trip_date_obj = datetime.strptime(trip_date, "%Y-%m-%d")

    if hours >= 24:
        hours -= 24
        trip_date_obj += timedelta(days=1)  # Move to the next day

    return datetime(trip_date_obj.year, trip_date_obj.month, trip_date_obj.day, hours, minutes, seconds)

def get_upcoming_trips(stop_times_df, start_stop, end_stop):
    """
    Find the next 3 departures **from the current time**.
    """
    current_time = datetime.now()
    today_str = current_time.strftime("%Y-%m-%d")
    tomorrow_str = (current_time + timedelta(days=1)).strftime("%Y-%m-%d")

    # Filter trips where the start and end stop both exist
    relevant_trips = stop_times_df[stop_times_df["stop_id"].isin([start_stop, end_stop])]

    trips_list = []
    
    for trip_id, group in relevant_trips.groupby("trip_id"):
        group = group.sort_values("stop_sequence")

        start_row = group[group["stop_id"] == start_stop]
        end_row = group[group["stop_id"] == end_stop]

        if not start_row.empty and not end_row.empty:
            dep_time = convert_gtfs_time(start_row["departure_time"].iloc[0], today_str)
            arr_time = convert_gtfs_time(end_row["departure_time"].iloc[0], today_str)

            if dep_time > current_time and dep_time < arr_time:
                trips_list.append((trip_id, dep_time.date(), dep_time, arr_time))

    # Sort trips by departure time
    trips_list.sort(key=lambda x: x[2])

    # Get the next 3 upcoming trips
    return trips_list[:3]

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

    # Get next 3 unique trips for each direction
    al_to_un_trips = get_upcoming_trips(stop_times_df, ALDERSHOT_STOP_ID, UNION_STOP_ID)
    un_to_al_trips = get_upcoming_trips(stop_times_df, UNION_STOP_ID, ALDERSHOT_STOP_ID)

    # Print the results
    print("\n🚆 Next 3 Departures: Aldershot → Union")
    print(f"{'Trip ID':<15} {'Date':<12} {'Aldershot Departure':<12} {'Union Arrival':<12}")
    for trip_id, date, dep, arr in al_to_un_trips:
        print(f"{trip_id:<15} {date:<12} {dep.strftime('%H:%M:%S'):<12} {arr.strftime('%H:%M:%S'):<12}")

    print("\n🚆 Next 3 Departures: Union → Aldershot")
    print(f"{'Trip ID':<15} {'Date':<12} {'Union Departure':<12} {'Aldershot Arrival':<12}")
    for trip_id, date, dep, arr in un_to_al_trips:
        print(f"{trip_id:<15} {date:<12} {dep.strftime('%H:%M:%S'):<12} {arr.strftime('%H:%M:%S'):<12}")

def main():
    fetch_gtfs()
    parse_gtfs()

if __name__ == "__main__":
    main()
