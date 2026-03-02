from src.transform import transform_readings


def test_transform_readings_single_record():
    """
    Happy-path test:
    - One input reading becomes one cleaned output record
    - Station name is injected correctly
    - Key fields are preserved
    """
    sample = [
        {
            "dateTime": "2024-01-01T10:00:00",
            "value": 5.2,
            "quality": "Good",
            "measure": {"@id": "test_measure"},
        }
    ]

    result = transform_readings(sample, "test_station")

    assert len(result) == 1
    assert result[0]["station"] == "test_station"
    assert result[0]["timestamp"] == "2024-01-01T10:00:00"
    assert result[0]["value"] == 5.2
    assert result[0]["quality"] == "Good"
    assert result[0]["measure_id"] == "test_measure"


def test_transform_empty_list_returns_empty_list():
    """
    Edge-case test:
    - If the API returns no readings, transform should return an empty list
    - Ensures the function doesn't crash and behaves predictably
    """
    result = transform_readings([], "test_station")
    assert result == []


def test_transform_missing_measure_does_not_crash_and_sets_measure_id_none():
    """
    Robustness test:
    - API data can be incomplete (e.g., missing nested 'measure')
    - We expect the code to avoid KeyError and set measure_id to None
    """
    sample = [
        {
            "dateTime": "2024-01-01T10:00:00",
            "value": 3.1,
            "quality": "Good",
            # measure is intentionally missing
        }
    ]

    result = transform_readings(sample, "test_station")

    assert len(result) == 1
    assert result[0]["measure_id"] is None
    assert result[0]["value"] == 3.1


def test_transform_multiple_records_preserves_count_and_order():
    """
    Loop/structure test:
    - Multiple input readings should become multiple output records
    - Ensures the function processes every item in the list
    - Confirms the output order matches input order (useful for debugging)
    """
    sample = [
        {
            "dateTime": "2024-01-01T10:00:00",
            "value": 1.0,
            "quality": "Good",
            "measure": {"@id": "m1"},
        },
        {
            "dateTime": "2024-01-01T11:00:00",
            "value": 2.0,
            "quality": "Unchecked",
            "measure": {"@id": "m2"},
        },
    ]

    result = transform_readings(sample, "test_station")

    assert len(result) == 2
    assert result[0]["timestamp"] == "2024-01-01T10:00:00"
    assert result[1]["timestamp"] == "2024-01-01T11:00:00"
    assert result[0]["measure_id"] == "m1"
    assert result[1]["measure_id"] == "m2"