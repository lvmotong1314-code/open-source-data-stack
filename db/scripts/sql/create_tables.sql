CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.taxi_trips (
    trip_id TEXT PRIMARY KEY,
    taxi_id TEXT,

    trip_start_timestamp TEXT,
    trip_end_timestamp TEXT,
    trip_seconds TEXT,
    trip_miles TEXT,

    pickup_census_tract TEXT,
    dropoff_census_tract TEXT,
    pickup_community_area TEXT,
    dropoff_community_area TEXT,

    fare TEXT,
    tips TEXT,
    tolls TEXT,
    extras TEXT,
    trip_total TEXT,

    payment_type TEXT,
    company TEXT,

    pickup_centroid_latitude TEXT,
    pickup_centroid_longitude TEXT,
    pickup_centroid_location JSONB,

    dropoff_centroid_latitude TEXT,
    dropoff_centroid_longitude TEXT,
    dropoff_centroid_location JSONB,

    _ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _batch_id TEXT NOT NULL,
    _source_name TEXT NOT NULL
);

