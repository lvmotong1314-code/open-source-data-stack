# Project Architecture

## Overview

This project implements a local batch data platform for the City of Chicago
Taxi Trips (2024-) dataset.

The current Week 5 architecture focuses on data ingestion, warehouse layering,
dbt transformation, dimensional modeling, automated data-quality validation,
source freshness, and lineage documentation.

```text
City of Chicago SODA 3 API
          |
          v
   taxi-ingestion
 Python + requests
     Psycopg 3
          |
          v
PostgreSQL: taxi_warehouse
          |
          v
   raw.taxi_trips
          |
          | dbt source()
          v
staging.stg_taxi_trips
          |
          | dbt ref()
          v
 +-----------------------+
 | marts.dim_date        |
 | marts.dim_company     |
 | marts.dim_location    |
 | marts.fct_taxi_trips  |
 +-----------------------+
```

## Runtime Components

### postgres_local

`postgres_local` is the PostgreSQL warehouse runtime for the project.

It hosts the `taxi_warehouse` database, which is organized into the following
schemas:

- `raw`
- `staging`
- `intermediate`
- `marts`

The warehouse uses a dedicated Docker volume so that database data persists
across container recreation.

### taxi-ingestion

`taxi-ingestion` is the Python ingestion runtime.

Its current responsibilities are:

- request Chicago Taxi Trips data from the SODA 3 API;
- receive source records as JSON;
- preserve source values at the raw boundary;
- adapt nested GeoJSON location objects to PostgreSQL `JSONB`;
- attach ingestion metadata;
- load records into `raw.taxi_trips`;
- prevent duplicate trip insertion using the source `trip_id`.

The raw loading strategy uses:

```text
trip_id PRIMARY KEY
+
ON CONFLICT (trip_id) DO NOTHING
```

This makes repeated ingestion attempts retry-safe with respect to duplicate
trip insertion.

Each inserted row also contains:

- `_batch_id`
- `_source_name`
- `_ingested_at`

### dbt

The dbt runtime is separated from the Airflow runtime.

Its current responsibilities are:

- register `raw.taxi_trips` as a dbt source;
- transform raw records into a typed staging model;
- build dimensional marts;
- execute automated data-quality tests;
- validate source freshness;
- generate documentation and lineage artifacts.

The current materialization strategy is:

```text
staging      -> view
intermediate -> view
marts        -> table
```

Airflow is intentionally not required for the current Week 5 implementation.

## Warehouse Layers

### Raw Layer

`raw.taxi_trips` is the ingestion boundary of the warehouse.

Its grain is:

> one row per unique source taxi trip

The raw layer preserves the source representation as closely as practical.

Most scalar API values are stored as `TEXT`.

Nested location objects are stored as PostgreSQL `JSONB`.

The raw layer also stores ingestion metadata:

- `_batch_id`
- `_source_name`
- `_ingested_at`

The raw layer does not perform analytical transformations.

### Staging Layer

`staging.stg_taxi_trips` provides a cleaned and typed representation of the raw
taxi records.

Its grain remains:

> one row per unique taxi trip

The staging layer performs basic normalization such as:

- raw timestamp text to PostgreSQL timestamp;
- numeric text to numeric or integer values;
- whitespace trimming;
- empty strings to SQL `NULL`;
- census tract normalization;
- preservation of ingestion metadata.

The staging layer intentionally avoids dimensional aggregation and business
metrics.

### Marts Layer

The marts layer implements the dimensional model.

The current marts are:

- `dim_date`
- `dim_company`
- `dim_location`
- `fct_taxi_trips`

#### fct_taxi_trips

The fact-table grain is:

> one row per unique taxi trip

`trip_id` is used as the fact grain key.

The fact contains:

- trip identifiers;
- dimension foreign keys;
- trip timestamps;
- payment type;
- trip duration and distance;
- fare-related measures;
- ingestion metadata.

The main foreign-key relationships are:

```text
start_date_key
    -> dim_date.date_key

end_date_key
    -> dim_date.date_key

pickup_location_key
    -> dim_location.location_key

dropoff_location_key
    -> dim_location.location_key

company_key
    -> dim_company.company_key
```

#### dim_date

`dim_date` has one row per calendar date.

Its primary key is a deterministic integer key in `YYYYMMDD` format.

For example:

```text
2026-09-09
    |
    v
20260909
```

`date_key = 0` is reserved for the Unknown Date member.

`dim_date` is role-played by:

- `start_date_key`;
- `end_date_key`.

#### dim_company

`dim_company` has one row per distinct normalized taxi company.

Company names receive deterministic hash surrogate keys.

Missing company values map to a single Unknown Company member.

Company names are lightly normalized using:

- whitespace trimming;
- empty-string-to-null conversion.

No SCD history or entity-resolution logic is implemented in the current
version.

#### dim_location

`dim_location` has one row per distinct canonical location.

A canonical location is represented by the combination of:

- community area;
- census tract;
- centroid latitude;
- centroid longitude.

Pickup and dropoff attributes are normalized into the same location structure.

`dim_location` is therefore role-played by:

- `pickup_location_key`;
- `dropoff_location_key`.

Partially missing location attributes remain valid location members.

For deterministic key generation, missing attributes use a technical sentinel
only while constructing the hash input.

If all canonical location attributes are missing, the fact maps to one
dedicated Unknown Location member.

## Deterministic Key Strategy

Dimension keys are designed to remain stable across rebuilds.

`dim_date` uses a deterministic integer key.

`dim_company` and `dim_location` use deterministic hash-based surrogate keys.

The same canonicalization and key-generation rules are used by both dimensions
and the fact table.

This allows fact-to-dimension mappings to remain stable during repeated dbt
builds.

## Data Quality

The project uses both generic dbt tests and project-specific singular tests.

### Generic Tests

Generic tests validate properties such as:

- key uniqueness;
- key non-nullability;
- fact-to-dimension relationships.

Examples include:

```text
fct_taxi_trips.trip_id
    -> unique
    -> not_null

fct_taxi_trips.company_key
    -> not_null
    -> relationships to dim_company.company_key
```

### Project-Specific Invariant Tests

Singular tests validate project-specific assumptions such as:

- exactly one Unknown Date member;
- exactly one Unknown Company member;
- exactly one Unknown Location member;
- the fact-table trip-id set matching the staging trip-id set.

These tests return zero rows when the invariant holds.

## Source Freshness

Source freshness is evaluated using:

```text
raw.taxi_trips._ingested_at
```

`_ingested_at` represents warehouse ingestion time rather than taxi-trip event
time.

The current freshness thresholds are:

```text
source age <= 24 hours
    -> PASS

source age > 24 hours
    -> WARN

source age > 48 hours
    -> ERROR
```

The project has verified both a stale-source failure and recovery after a new
ingestion successfully inserted records.

## Lineage

dbt lineage is derived from `source()` and `ref()` dependencies.

The current execution lineage is:

```text
raw.taxi_trips
      |
      v
stg_taxi_trips
      |
      +--> dim_date
      |
      +--> dim_company
      |
      +--> dim_location
      |
      +--> fct_taxi_trips
```

The fact-to-dimension relationships are logical dimensional relationships.

They are validated using dbt `relationships` tests.

These relationships are distinct from dbt execution dependencies because the
fact and dimensions independently derive the same deterministic keys from
staging attributes.

## Current Design Tradeoffs

The current implementation makes several deliberate tradeoffs.

Duplicate source records are handled with:

```text
ON CONFLICT (trip_id) DO NOTHING
```

This prioritizes retry-safe ingestion but means corrections to an already
ingested source trip are not synchronized automatically.

Dimension keys are deterministic rather than sequence-generated so that
rebuilds preserve stable key mappings.

The project does not introduce `dim_taxi` because the source currently provides
a taxi identifier but no meaningful descriptive taxi attributes.

The project does not introduce `dim_payment_type` because payment type is a
small categorical attribute and does not require a separate dimension in the
current version.

The project also keeps dbt execution independent from Airflow during Week 5 so
that transformation concepts can be developed and validated before
orchestration is introduced.

## Future Extension Points

The current implementation intentionally stops at the Week 5 project boundary.

Future stages are expected to extend the platform with:

- Airflow orchestration;
- scheduling and retries;
- timeout and catchup configuration;
- deterministic backfill workflows;
- pipeline-level failure recovery;
- dbt tests as orchestration quality gates;
- CI checks;
- PySpark raw processing;
- Parquet output;
- partitioning experiments;
- runtime and recovery metrics;
- final reproducibility documentation.

These components are future extension points and are not claimed as implemented
in the current Week 5 version.