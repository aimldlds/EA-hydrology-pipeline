from src.api import fetch_measurements
from src.database import (
    get_connection,
    create_tables,
    insert_station,
    insert_measurements
)


def run_pipeline(station_name: str):
    """
    Orchestrates full ETL pipeline.
    """

    print("Fetching data...")
    station_id, station_label, readings = fetch_measurements(station_name)

    # Create database connection
    conn = get_connection()

    print("Creating tables...")
    create_tables(conn)

    print("Inserting station...")
    insert_station(conn, station_id, station_label)

    print("Inserting measurements...")
    insert_measurements(conn, station_id, readings)

    conn.close()

    print(f"Inserted {len(readings)} measurements.")