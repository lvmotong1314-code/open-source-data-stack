
from uuid import uuid4
import logging
import os

import requests
import psycopg
from psycopg.types.json import Jsonb


API_URL = "https://data.cityofchicago.org/api/v3/views/ajtu-isnz/query.json"
SOURCE_NAME = "chicago_taxi_trips_2024_plus"
SOURCE_COLUMNS = (
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
)

INSERT_SQL = """
    INSERT INTO raw.taxi_trips (
        trip_id,
        taxi_id,
        trip_start_timestamp,
        trip_end_timestamp,
        trip_seconds,
        trip_miles,
        pickup_census_tract,
        dropoff_census_tract,
        pickup_community_area,
        dropoff_community_area,
        fare,
        tips,
        tolls,
        extras,
        trip_total,
        payment_type,
        company,
        pickup_centroid_latitude,
        pickup_centroid_longitude,
        pickup_centroid_location,
        dropoff_centroid_latitude,
        dropoff_centroid_longitude,
        dropoff_centroid_location,
        _batch_id,
        _source_name
    )
    VALUES (
        %(trip_id)s,
        %(taxi_id)s,
        %(trip_start_timestamp)s,
        %(trip_end_timestamp)s,
        %(trip_seconds)s,
        %(trip_miles)s,
        %(pickup_census_tract)s,
        %(dropoff_census_tract)s,
        %(pickup_community_area)s,
        %(dropoff_community_area)s,
        %(fare)s,
        %(tips)s,
        %(tolls)s,
        %(extras)s,
        %(trip_total)s,
        %(payment_type)s,
        %(company)s,
        %(pickup_centroid_latitude)s,
        %(pickup_centroid_longitude)s,
        %(pickup_centroid_location)s,
        %(dropoff_centroid_latitude)s,
        %(dropoff_centroid_longitude)s,
        %(dropoff_centroid_location)s,
        %(_batch_id)s,
        %(_source_name)s
    )
    ON CONFLICT (trip_id) DO NOTHING
"""


logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler()],
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


def get_app_token():
    token = os.getenv("CHICAGO_APP_TOKEN")

    if not token:
        raise RuntimeError("CHICAGO_APP_TOKEN is not set")

    return token


def get_db_config():
    config = {
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
    }

    missing = [
        key
        for key, value in config.items()
        if not value
    ]

    if missing:
        logger.error(
            "Missing PostgreSQL configuration: %s",
            ", ".join(missing),
        )
        raise RuntimeError(
            f"Missing PostgreSQL configuration: {', '.join(missing)}"
        )

    return config


def fetch_taxi_trips(page_number=1, page_size=10):
    token = get_app_token()

    headers = {
        "X-App-Token": token,
    }

    payload = {
        "query": f"SELECT * LIMIT {page_size}",
        "includeSynthetic": False,
        "orderingSpecifier": "discard",
}

    logger.info(
        "Requesting Chicago Taxi Trips page=%s page_size=%s",
        page_number,
        page_size,
    )

    response = requests.post(
        API_URL,
        headers=headers,
        json=payload,
        timeout=30,
    )

    response.raise_for_status()

    records = response.json()

    if not isinstance(records, list):
        raise TypeError(
            f"Expected API response to be a list, got {type(records).__name__}"
        )

    logger.info("Received %s taxi trip records", len(records))

    return records

def prepare_record(record, batch_id):
    prepared = {
        column: record.get(column)
        for column in SOURCE_COLUMNS
    }

    if prepared["pickup_centroid_location"] is not None:
        prepared["pickup_centroid_location"] = Jsonb(
            prepared["pickup_centroid_location"]
        )

    if prepared["dropoff_centroid_location"] is not None:
        prepared["dropoff_centroid_location"] = Jsonb(
            prepared["dropoff_centroid_location"]
        )

    prepared["_batch_id"] = batch_id
    prepared["_source_name"] = SOURCE_NAME

    return prepared


def load_taxi_trips(records):
    if not records:
        logger.info("No records to load")
        return None, 0

    batch_id = str(uuid4())
    db_config = get_db_config()

    prepared_records = [
        prepare_record(record, batch_id)
        for record in records
    ]

    logger.info(
        "Loading %s records into raw.taxi_trips batch_id=%s",
        len(prepared_records),
        batch_id,
    )

    with psycopg.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                INSERT_SQL,
                prepared_records,
            )

            cur.execute(
                """
                SELECT COUNT(*)
                FROM raw.taxi_trips
                WHERE _batch_id = %s
                """,
                (batch_id,),
            )

            inserted_count = cur.fetchone()[0]

    logger.info(
        "Batch complete batch_id=%s fetched=%s inserted=%s skipped=%s",
        batch_id,
        len(records),
        inserted_count,
        len(records) - inserted_count,
    )

    return batch_id, inserted_count


if __name__ == "__main__":
    trips = fetch_taxi_trips()
    load_taxi_trips(trips)

    if trips:
        logger.info("First record contains %s fields", len(trips[0]))
        logger.info("First record keys: %s", sorted(trips[0].keys()))