# Hydrology Data Pipeline

## Overview

This project implements a simple ETL data engineering pipeline that:

- Connects to the UK Hydrological Data Explorer API  
- Retrieves the 10 most recent measurements for two parameters from the station **HIPPER_PARK ROAD BRIDGE_E_202312**  
- Transforms the data  
- Loads it into a file-based SQLite database  
- Stores the data using a simple star schema design  
- Is designed to run via a single command  
- Follows data engineering best practices  

## Architecture

The pipeline follows a standard ETL pattern:

Extract → Transform → Load

### 1. Extract

- Connects to the Hydrology API  
- Finds the station GUID dynamically  
- Retrieves 10 most recent readings for two parameters  

### 2. Transform

- Cleans parameter names  
- Normalises field naming  
- Prepares data for relational storage  

### 3. Load

- Creates SQLite database  
- Builds star schema tables  
- Inserts station and measurement records  

## Database Design (Star Schema)

The database follows a simple dimensional model.

### Dimension Table: `stations`

| Column | Description |
|--------|------------|
| station_id (PK) | Unique station identifier |
| station_name | Station label |

### Fact Table: `measurements`

| Column | Description |
|--------|------------|
| id (PK) | Measurement ID |
| station_id (FK) | Links to stations |
| timestamp | Measurement time |
| parameter | Parameter name |
| value | Numeric measurement value |
| quality | Data quality flag |

### Foreign Key Relationship

`measurements.station_id → stations.station_id`

## Project Structure

```
hydrology_pipeline/
│
├── src/
│   ├── api.py
│   ├── database.py
│   ├── pipeline.py
│   └── transform.py
│
├── tests/
│   └── test_transform.py
│
├── main.py
├── requirements.txt
├── pytest.ini
└── README.md
```


## How To Run

### 1. Clone the Repository

Run the following commands in your terminal:

git clone <your-repo-url>
cd hydrology_pipeline

### 2. Create a Virtual Environment (Recommended)

#### Mac / Linux

python3 -m venv .venv
source .venv/bin/activate

#### Windows

python -m venv .venv
.venv\Scripts\activate

### 3. Install Dependencies

pip install -r requirements.txt

### 4. Run the Pipeline

python main.py

You should see output similar to:

Using GUID: ...
Fetching: ...
Fetching: ...
Creating tables...
Inserted 20 measurements.

A file named hydrology.db will be created in the project root.

### 5. Verify the Database

sqlite3 hydrology.db

.tables

SELECT * FROM stations;
SELECT * FROM measurements LIMIT 5;

.exit


## Running Tests
- This project includes unit tests.
- Run: pytest
- All tests should pass.

## Design Decisions
- Functional programming structure
- Clear separation of concerns (API / transform / database)
- Pagination support for station search
- Dynamic parameter selection
- Snake_case naming for SQL consistency
- Single-command execution
- SQLite for portability (Windows compatible)

## Dependencies
- requests
- pytest
- See requirements.txt for full list.
