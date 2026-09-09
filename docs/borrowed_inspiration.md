# Borrowed Inspiration and Original Work

## Scaffold

This project started from the repository structure of
`luchonaveiro/open-source-data-stack`.

The original repository was used as an architectural scaffold and learning
reference rather than as the finished project implementation.

## Borrowed Inspiration

The following ideas were retained or adapted from the scaffold:

- Docker Compose as the local multi-service development environment;
- separation between a PostgreSQL warehouse and transformation tooling;
- environment-variable-based PostgreSQL configuration;
- the general dbt project organization of staging, intermediate, and marts;
- the pattern of declaring external warehouse tables as dbt sources;
- dbt documentation and lineage generation;
- the broader idea of integrating ingestion, transformation, data quality,
  and later orchestration in one repository.

## Replaced Tutorial Content

The original Jaffle Shop and Stripe tutorial implementation was removed.

This includes the original:

- tutorial CSV datasets;
- Jaffle Shop ingestion script and tables;
- Jaffle Shop and Stripe dbt models;
- tutorial source definitions and tests;
- generated tutorial dbt artifacts.

The project no longer uses the original tutorial business process or schema.

## Original Project Work

The following components were designed or rewritten for this project:

### Data Source

The project uses the City of Chicago Taxi Trips (2024-) dataset through the
SODA 3 API.

The source schema was independently profiled and mapped into the warehouse.

### Ingestion Contract

The raw contract was redesigned around:

- `taxi_warehouse.raw.taxi_trips`;
- one row per source taxi trip;
- `trip_id` as the raw primary key;
- retry-safe duplicate handling;
- `_batch_id`;
- `_source_name`;
- `_ingested_at`;
- raw `JSONB` preservation for nested location objects.

### dbt Runtime

A standalone dbt runtime was introduced instead of requiring dbt execution
through the original Airflow image.

This keeps Week 5 transformation work independent from orchestration concepts
that are introduced later in the project.

### Warehouse Model

The dimensional model was designed specifically for Chicago Taxi Trips:

- `fct_taxi_trips`;
- `dim_date`;
- `dim_company`;
- `dim_location`.

The model includes:

- explicit fact and dimension grains;
- role-playing date dimensions;
- role-playing location dimensions;
- deterministic dimension keys;
- explicit Unknown dimension members;
- deliberate decisions not to introduce `dim_taxi` or `dim_payment_type`
  in the initial version.

### Transformation Rules

The staging layer was redesigned to perform source-specific normalization,
including:

- timestamp conversion;
- numeric conversion;
- census-tract normalization;
- empty-string handling;
- preservation of source and ingestion metadata.

### Data Quality

Project-specific quality rules include:

- source identity and ingestion-metadata tests;
- fact and dimension key tests;
- fact-to-dimension relationship tests;
- Unknown-member invariants;
- staging-to-fact trip-set equality;
- source freshness based on ingestion time.

### Engineering Tradeoffs

The project intentionally favors deterministic, rebuildable modeling and a
clear raw boundary.

Current tradeoffs include:

- duplicate source `trip_id` values are ignored rather than updated;
- deterministic hash keys are used instead of sequence-generated dimension
  keys;
- source corrections to previously ingested trips are not synchronized in the
  current raw-loading strategy;
- Airflow orchestration and Spark processing are deferred to later project
  stages.

## Project Positioning

The project is not intended to differentiate itself through the use of a rare
dataset.

Its engineering focus is the construction of a reproducible, retry-safe,
backfillable local batch data platform whose behavior, contracts, data quality,
and failure-recovery decisions can be explicitly explained and tested.