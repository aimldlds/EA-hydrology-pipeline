import requests

# Base endpoint for the Hydrology API.
BASE = "https://environment.data.gov.uk/hydrology/id/stations"


def find_station_guid(station_name: str):
    """
    Searches through the Hydrology API station list
    and dynamically finds the GUID for a station
    based on a partial name match.

    Why?
    - The requirement specifies a station name.
    - The API requires a GUID to fetch detailed data.
    - So we first search for the station and extract its GUID.
    """

    # Pagination setup.
    # The API returns stations in pages.
    # We use pagination to ensure we search through ALL stations.

    offset = 0
    limit = 200 # 200 per request

    while True:
        # Construct paginated request URL
        url = f"{BASE}.json?_limit={limit}&_offset={offset}"
        print("Checking:", url)
        
        # Make API request
        r = requests.get(url)
        r.raise_for_status() # raise_for_status() ensures the program fails, immediately if API returns error

        data = r.json()
        items = data.get("items", [])

        if not items: # If no items returned, we reached the end
            break

        for item in items:

            label = item.get("label", "") # Extract station label
            
            # Sometimes label may be a list
            # Normalize it to string
            if isinstance(label, list):
                label = " ".join(label)
            
            # Convert to lowercase for case-insensitive matching
            label = str(label).lower()
            
            # Partial match allows flexible searching
            if station_name.lower() in label:
                print("Matched station:", label)
                
                # Extract GUID from "@id"
                # Example:
                # https://.../stations/E64999A
                station_id = item.get("@id")

                if station_id:
                    guid = station_id.split("/")[-1]
                    return guid

        offset += limit # Move to next page

    return None # If no station found


def fetch_measurements(station_name: str):
    """
    Main extraction function.

    Steps:
    1. Find station GUID
    2. Retrieve station details
    3. Select two unique parameters
    4. Fetch 10 most recent readings per parameter
    5. Return cleaned structured data
    """
    # Step 1: Get GUID
    guid = find_station_guid(station_name)

    if not guid:
        raise ValueError(f"Station not found: {station_name}")

    print("Using GUID:", guid)
    
    # Step 2: Fetch full station metadata
    station_url = f"{BASE}/{guid}.json"
    station_resp = requests.get(station_url)
    station_resp.raise_for_status()

    station_data = station_resp.json()
    items = station_data.get("items")

    if isinstance(items, list):
        items = items[0]
    
    # Extract measures (parameters)
    measures = items.get("measures")
    
    # Ensure measures is always a list
    if not isinstance(measures, list):
        measures = [measures]

    # ---------------------------
    # Parameter Selection Logic
    # ---------------------------

    # We select first 2 UNIQUE parameters.
    # Why unique?
    # Because API may contain duplicate parameter types
    # (e.g., daily vs sub-daily versions).
    selected_measures = []
    seen_parameters = set()

    for measure in measures:
        full_label = measure.get("label", "")
        
        # Clean parameter name.
        # Example label:
        # "Sub-daily Dissolved Oxygen (mg/L) time series for ..."
        try:
            parameter_name = full_label.split("Sub-daily ")[1].split(" (")[0]
        except IndexError:
            parameter_name = full_label # Fallback if format unexpected
        
         # Deduplicate parameters
        if parameter_name not in seen_parameters:
            seen_parameters.add(parameter_name)
            selected_measures.append((measure, parameter_name))
        
        # Stop after 2 parameters
        if len(selected_measures) == 2:
            break
    
    # ---------------------------
    # Fetch Readings
    # ---------------------------
    all_readings = []

    for measure, parameter_label in selected_measures:
        
        # Each measure has its own endpoint
        measure_id = measure["@id"]
        # Requirement: fetch 10 most recent readings
        readings_url = measure_id + "/readings.json?_limit=10"

        print("Fetching:", readings_url)

        r = requests.get(readings_url)
        r.raise_for_status()

        readings = r.json().get("items", [])
        
        # Attach cleaned parameter name(eg., Conductivity, Dissolved Oxygen) to each reading
        # This makes transformation step easier to read
        for reading in readings:
            reading["parameter"] = parameter_label

        all_readings.extend(readings)
    
    # Return structured output to pipeline layer
    return guid, items.get("label"), all_readings