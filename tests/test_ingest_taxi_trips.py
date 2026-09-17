import sys
from pathlib import Path

from psycopg.types.json import Jsonb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "db" / "scripts"))

from ingest_taxi_trips import SOURCE_NAME, prepare_record


def test_prepare_record_adds_ingestion_metadata():
    record = {
        "trip_id": "test-trip-001",
        "taxi_id": "test-taxi-001",
        "pickup_centroid_location": {
            "type": "Point",
            "coordinates": [-87.6270, 41.8810],
        },
        "dropoff_centroid_location": None,
    }

    prepared = prepare_record(record, "test-batch-001")

    assert prepared["trip_id"] == "test-trip-001"
    assert prepared["taxi_id"] == "test-taxi-001"
    assert prepared["_batch_id"] == "test-batch-001"
    assert prepared["_source_name"] == SOURCE_NAME
    assert isinstance(prepared["pickup_centroid_location"], Jsonb)
    assert prepared["dropoff_centroid_location"] is None