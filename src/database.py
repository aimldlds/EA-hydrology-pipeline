import sqlite3


def create_tables(conn):
    """
    Creates the star schema tables in SQLite.

    Design:
    - stations → Dimension table
    - measurements → Fact table

    Why separate?
    - Avoid data duplication
    - Follow dimensional modelling 
    - Enable analytical queries via joins
    """

    cursor = conn.cursor()

    # ---------------------------
    # Dimension Table: stations
    # ---------------------------
    # Stores descriptive information about stations.
    # Low volume, slowly changing.
    # Primary key ensures uniqueness.

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stations (
            station_id TEXT PRIMARY KEY,
            station_name TEXT
        )
    """)

    # ---------------------------
    # Fact Table: measurements
    # ---------------------------
    # Stores time-series numerical data.
    # High volume table.
    # Linked to stations via foreign key.
    # AUTOINCREMENT provides unique surrogate key.

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            station_id TEXT,
            timestamp TEXT,
            parameter TEXT,
            value REAL,
            quality TEXT,
            FOREIGN KEY (station_id) REFERENCES stations(station_id)
        )
    """)

    conn.commit()


def insert_station(conn, station_id, station_name):
    """
    Inserts a station record into the dimension table.

    Why INSERT OR IGNORE?
    - Prevents duplicate stations if pipeline is rerun.
    - Makes pipeline idempotent (safe to run multiple times).
    """

    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO stations (station_id, station_name)
        VALUES (?, ?)
    """, (station_id, station_name))

    conn.commit()


def insert_measurements(conn, station_id, readings):
    """
    Inserts measurement records into fact table.

    Parameters:
    - station_id → foreign key reference
    - readings → list of transformed API readings

    Each reading contains:
    - timestamp
    - parameter
    - value
    - quality
    """

    cursor = conn.cursor()

    # Loop through each reading and insert into fact table
    for reading in readings:

        cursor.execute("""
            INSERT INTO measurements 
            (station_id, timestamp, parameter, value, quality)
            VALUES (?, ?, ?, ?, ?)
        """, (
            station_id,
            reading.get("dateTime"),     # from API
            reading.get("parameter"),    # cleaned earlier
            reading.get("value"),
            reading.get("quality")
        ))

    conn.commit()


def get_connection(db_name="hydrology.db"):
    """
    Creates SQLite database connection.
    
    """

    return sqlite3.connect(db_name)