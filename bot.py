import pandas as pd
import os

GTFS_DIR = "gtfs_data"
stops_path = os.path.join(GTFS_DIR, "stops.txt")

# Load stops.txt
stops_df = pd.read_csv(stops_path)

# Print all stop names
print(stops_df[["stop_id", "stop_name"]].to_string(index=False))

