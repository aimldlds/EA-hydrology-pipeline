def transform_readings(readings, station_name: str):
    """
    Transform raw API hydrology readings into clean, structured records
    suitable for relational database storage.

    Purpose:
    - Normalise field names
    - Select only required attributes
    - Prepare data for loading into fact table
    """

    cleaned = []

    # Loop through each raw reading returned by API
    for r in readings:

        # Build a clean, structured dictionary
        cleaned.append({
            # Add station context explicitly
            # Even though station_id exists later,
            # this keeps transformation independent of DB logic
            "station": station_name,

            # Rename API camelCase field to snake_case
            # This improves SQL consistency
            "timestamp": r.get("dateTime"),

            # Numeric measurement
            "value": r.get("value"),

            # Quality flag from API
            "quality": r.get("quality"),

            # Store original measure identifier
            # Useful for traceability / debugging
            "measure_id": r.get("measure", {}).get("@id")
        })

    return cleaned