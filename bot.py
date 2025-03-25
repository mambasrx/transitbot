import os
import requests
import zipfile
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta

# GTFS Feed URL
GTFS_URL = "https://assets.metrolinx.com/raw/upload/Documents/Metrolinx/Open%20Data/GO-GTFS.zip"
GTFS_DIR = "gtfs_data"

# Define Stop Names (Matched from stops.txt)
ALDERSHOT_STOP_NAME = "Aldershot GO"
UNION_STOP_NAME = "Union Station GO"

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
    stops_path = os.path.join(GTFS_DIR, "stops.txt")
    stop_times_path = os.path.join(GTFS_DIR, "stop_times.txt")
    trips_path = os.path.join(GTFS_DIR, "trips.txt")
    routes_path = os.path.join(GTFS_DIR, "routes.txt")

    try:
        # Load stops.txt to find stop IDs
        stops_df = pd.read_csv(stops_path)
        ald_stop_id = stops_df.loc[stops_df["stop_name"] == ALDERSHOT_STOP_NAME, "stop_id"].values
        union_stop_id = stops_df.loc[stops_df["stop_name"] == UNION_STOP_NAME, "stop_id"].values
        
        if len(ald_stop_id) == 0 or len(union_stop_id) == 0:
            raise ValueError("❌ Could not find stop IDs for Aldershot or Union in stops.txt.")
        
        ald_stop_id, union_stop_id = ald_stop_id[0], union_stop_id[0]

        # Load GTFS stop_times.txt, trips.txt, and routes.txt
        stop_times_df = pd.read_csv(stop_times_path, low_memory=False)
        trips_df = pd.read_csv(trips_path, low_memory=False)
        routes_df = pd.read_csv(routes_path, low_memory=False)

        # Identify all trip IDs for Lakeshore West (route_name contains 'Lakeshore West')
        lakeshore_trips = trips_df[trips_df["route_id"].isin(
            routes_df[routes_df["route_long_name"].str.contains("Lakeshore West", na=False)]["route_id"]
        )]

        # Merge stop_times with relevant trips
        merged_df = stop_times_df.merge(lakeshore_trips, on="trip_id")

        # Convert departure_time to datetime
        merged_df["departure_time"] = pd.to_datetime(merged_df["departure_time"], errors="coerce")
        now = datetime.now()
        future_time = now + timedelta(minutes=90)

        # Filter for trips that include both Aldershot & Union
        relevant_trips = merged_df[merged_df["stop_id"].isin([ald_stop_id, union_stop_id])]

        # Identify trips traveling in each direction
        grouped_trips = relevant_trips.groupby("trip_id")
        ald_to_union_trips = []
        union_to_ald_trips = []

        for trip_id, group in grouped_trips:
            group = group.sort_values("stop_sequence")  # Ensure stop order
            stops = group["stop_id"].values

            if ald_stop_id in stops and union_stop_id in stops:
                ald_index = list(stops).index(ald_stop_id)
                union_index = list(stops).index(union_stop_id)

                if ald_index < union_index:
                    ald_to_union_trips.append(trip_id)
                else:
                    union_to_ald_trips.append(trip_id)

        # Filter stop_times for upcoming departures in each direction
        ald_to_union = merged_df[
            (merged_df["trip_id"].isin(ald_to_union_trips)) &
            (merged_df["stop_id"] == ald_stop_id) &  # Get departures from Aldershot
            (merged_df["departure_time"] >= now) &
            (merged_df["departure_time"] <= future_time)
        ].sort_values("departure_time")

        union_to_ald = merged_df[
            (merged_df["trip_id"].isin(union_to_ald_trips)) &
            (merged_df["stop_id"] == union_stop_id) &  # Get departures from Union
            (merged_df["departure_time"] >= now) &
            (merged_df["departure_time"] <= future_time)
        ].sort_values("departure_time")

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
