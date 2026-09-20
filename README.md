# Chicago Taxi Data Platform

A local-first batch data engineering project built on the City of Chicago Taxi Trips dataset.

The project implements API ingestion, PostgreSQL raw storage, dbt dimensional modeling and data-quality validation, Airflow orchestration, GitHub Actions CI, and an independent PySpark batch-processing path that writes partitioned Parquet data.

The project is designed as an intern-level data engineering / data platform project and emphasizes reproducibility, explicit data-quality checks, failure handling, and documented engineering trade-offs.

---

## Architecture

```text
                         City of Chicago SODA 3 API
                                    |
                                    v
                              taxi-ingestion
                             Python + requests
                                    |
                                    v
                         PostgreSQL: taxi_warehouse
                                    |
                              raw.taxi_trips
                              /             \
                             /               \
                            v                 v
                          dbt               PySpark
                           |              local[*] + JDBC
                           v                 |
                 staging.stg_taxi_trips      v
                           |          type normalization
                           v          + derive trip_date
                         marts                |
                 +---------+---------+        v
                 |         |         |     Parquet
              dim_date  dim_company  ...   partitioned by
                         dim_location       trip_date
                 |
                 v
             fct_taxi_trips
```

Airflow orchestrates the ingestion, dbt transformation, data-quality, scheduling, retry, timeout, catchup/backfill, and failure-recovery workflow.

The PySpark batch job is currently independently executable through `spark-submit`. It reads directly from `raw.taxi_trips` through JDBC and is not currently scheduled as an Airflow task.

Detailed architecture documentation is available in:

`docs/architecture.md`

---

## Tech Stack

| Component | Role |
|---|---|
| Python | API ingestion and pipeline logic |
| Socrata SODA 3 API | Chicago Taxi Trips source |
| PostgreSQL | Raw and analytical warehouse storage |
| dbt | Staging models, dimensional marts, tests, freshness, and lineage |
| Apache Airflow | Workflow orchestration, scheduling, retries, and backfill |
| PySpark | Batch transformation and Parquet processing |
| Apache Parquet | Columnar batch-storage format |
| Docker Compose | Local runtime and service isolation |
| GitHub Actions | Continuous integration |
| pytest | Python ingestion tests |

Spark currently runs in local mode using:

`local[*]`

The project does not deploy a distributed Spark cluster.

---

## Dataset

The project uses the City of Chicago Taxi Trips dataset through the SODA 3 API.

The ingestion layer:

- sends authenticated API requests using an app token;
- supports paginated retrieval;
- uses timeout and error handling;
- logs ingestion activity;
- writes source records into PostgreSQL;
- uses `trip_id` as the raw-table primary key;
- avoids duplicate inserts through conflict handling;
- records ingestion metadata.

The current local development dataset contains approximately 20,000 taxi-trip records.

The dataset is intentionally small enough to run locally. Spark experiments are used to demonstrate batch-processing and physical-layout behavior rather than production-scale performance.

---

## Pipeline Components

### Python Ingestion

The ingestion code is located under:

`db/scripts/`

The ingestion process reads Chicago Taxi Trips data from the SODA 3 API and writes it into:

`raw.taxi_trips`

The raw layer preserves source values as closely as practical while also storing ingestion metadata including:

- `_batch_id`
- `_source_name`
- `_ingested_at`

The ingestion logic is designed to be safe to retry.

---

### PostgreSQL

The local PostgreSQL service hosts:

`taxi_warehouse`

The warehouse is organized into the following schemas:

- `raw`
- `staging`
- `intermediate`
- `marts`

The raw ingestion boundary is:

`raw.taxi_trips`

Its grain is:

> one row per unique source taxi trip

---

## dbt Transformation

The dbt project is located at:

`dbt/chicago_taxi`

The transformation path is:

```text
raw.taxi_trips
        |
        v
staging.stg_taxi_trips
        |
        v
      marts
```

The current dimensional model includes:

- `marts.dim_date`
- `marts.dim_company`
- `marts.dim_location`
- `marts.fct_taxi_trips`

The fact-table grain is:

> one row per unique taxi trip

dbt is also used for automated data-quality validation including tests such as:

- `not_null`
- `unique`
- `relationships`
- custom project tests

Source freshness and dbt documentation / lineage are also configured.

---

## Airflow Orchestration

Airflow provides the orchestration layer for the Project 1 pipeline.

The DAG is located at:

`airflow/dags/project1_taxi_pipeline.py`

The implemented workflow covers:

- task dependencies;
- scheduled execution;
- retries;
- timeout behavior;
- catchup;
- historical backfill;
- failure and recovery validation;
- dbt-based quality gates.

The Airflow environment runs through Docker Compose.

The PySpark batch job remains independently executable and is not currently an Airflow DAG task.

---

## PySpark Batch Processing

The Spark job is located at:

`spark/jobs/taxi_batch.py`

The job performs the following workflow:

```text
PostgreSQL raw.taxi_trips
          |
          | JDBC
          v
     Spark DataFrame
          |
          v
   field selection
   numeric type casts
   derive trip_date
          |
          v
 partitioned Parquet
```

The Spark job:

- reads `raw.taxi_trips` from PostgreSQL through JDBC;
- selects the fields required by the batch-processing path;
- converts duration, distance, and fare-related fields to numeric types;
- derives `trip_date` from `trip_start_timestamp`;
- writes Snappy-compressed Parquet;
- partitions the final output by `trip_date`;
- rereads the Parquet output;
- validates the output row count against the PostgreSQL source row count.

Run the Spark batch job with:

```bash
docker compose run --rm spark spark-submit --jars /opt/jdbc/postgresql.jar spark/jobs/taxi_batch.py
```

The generated output is written under:

`data/parquet/`

Generated Parquet files are ignored by Git because they are reproducible runtime artifacts.

---

## Parquet Partition Experiment

Several physical layouts were evaluated during Week 7.

| Layout | Parquet Files | Approximate Size |
|---|---:|---:|
| Unpartitioned | 1 | 1.1 MB |
| Partitioned by `trip_date` | 12 | 1.3 MB |
| Partitioned by `payment_type` | 8 | 1.3 MB |

### Date Partitioning

The `trip_date` layout showed substantial data skew.

Most records were concentrated in a small number of dates, while several historical date partitions contained only one or two rows and produced very small Parquet files.

For a query filtering on a specific date, Spark `EXPLAIN` showed:

- directory pruning;
- `PartitionFilters`;
- elimination of unrelated date partitions before Parquet file scanning.

The unpartitioned layout instead used Parquet predicate pushdown because `trip_date` remained a normal column inside the Parquet file.

### Design Decision

For the current small dataset, an unpartitioned layout has the lowest file-management overhead.

However, the final Spark batch path uses `trip_date` as the partition key because it matches the time-oriented growth pattern of a recurring taxi-trip pipeline and supports partition pruning for date-oriented reads.

The experiment also demonstrates that partitioning is not automatically beneficial.

Partition design must consider:

1. query access patterns;
2. partition cardinality;
3. data distribution and skew;
4. expected dataset growth;
5. resulting file count and file size.

Detailed results are documented in:

`docs/week7/partition_tradeoff.md`

---

## Spark Resource Observation

The Spark batch job was monitored during local execution.

Observed resource categories included:

- CPU utilization;
- memory utilization;
- Docker network I/O;
- block I/O.

Because Spark runs using `local[*]`, the driver and local executor tasks share the same Spark container.

Network I/O increases while Spark reads PostgreSQL through JDBC, while block I/O is generated during Parquet writes and read-back validation.

These measurements are intended as evidence of local execution behavior and are not presented as production throughput benchmarks.

Detailed observations are documented in:

`docs/week7/resource_observation.md`

---

## Data Quality

Data quality is enforced at multiple layers.

### Ingestion

The ingestion layer validates API responses and relies on PostgreSQL constraints and conflict handling to prevent duplicate raw records.

### dbt

dbt tests validate analytical models, column constraints, and dimensional relationships.

### Spark

The Spark batch job rereads the generated Parquet output and verifies:

```text
source row count == Parquet row count
```

A mismatch causes the Spark application to fail instead of silently publishing incomplete output.

---

## Continuous Integration

GitHub Actions runs automatically on:

- `push`
- `pull_request`

The current CI workflow uses a lightweight PostgreSQL fixture and validates:

- Python dependency installation;
- Python ingestion unit tests;
- Airflow DAG syntax;
- raw database initialization;
- CI fixture loading;
- dbt dependency installation;
- dbt compilation;
- dbt model execution;
- dbt tests.

The CI fixture intentionally contains only a small amount of data because its purpose is integration correctness rather than performance testing.

The workflow is located at:

`.github/workflows/ci.yml`

The Spark batch job is executed and validated locally. A lightweight Spark CI smoke test can be added without using the full local development dataset.

---

## Running the Project

### Start Local Services

```bash
docker compose up -d
```

### Run Taxi Ingestion

```bash
docker compose run --rm taxi-ingestion
```

### Run dbt

```bash
docker compose run --rm dbt dbt build --profiles-dir .
```

### Run the Spark Batch Job

```bash
docker compose run --rm spark spark-submit --jars /opt/jdbc/postgresql.jar spark/jobs/taxi_batch.py
```

Airflow services are managed through Docker Compose, and the DAG can be monitored through the local Airflow web interface.

---

## Project Structure

```text
.
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── airflow/
│   └── dags/
│       └── project1_taxi_pipeline.py
│
├── db/
│   ├── scripts/
│   │   ├── ingest_taxi_trips.py
│   │   └── sql/
│   │       └── create_tables.sql
│
├── dbt/
│   └── chicago_taxi/
│       ├── analyses/
│       ├── macros/
│       ├── models/
│       ├── snapshots/
│       └── tests/
│
├── docs/
│   ├── architecture.md
│   ├── data_contract.md
│   ├── data_model.md
│   └── week7/
│       ├── partition_tradeoff.md
│       └── resource_observation.md
│
├── spark/
│   └── jobs/
│       └── taxi_batch.py
│
├── tests/
│   ├── fixtures/
│   │   └── ci_taxi_trips.sql
│   └── test_ingest_taxi_trips.py
│
├── airflow.Dockerfile
├── db.Dockerfile
├── dbt.Dockerfile
├── spark.Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Current Design Boundaries

The current implementation intentionally has several local-development boundaries.

- Spark runs in local mode rather than on a distributed cluster.
- The Spark batch path is independently executable and is not currently orchestrated by Airflow.
- Parquet output is stored on local project storage rather than object storage.
- The local dataset is intentionally small.
- Partition experiments are not presented as production performance benchmarks.
- Project 1 has not yet been frozen as a resume-ready `v1.0` release.

Final release polish and Project 1 `v1.0` are intentionally left for the next project stage.

---

## Borrowed Inspiration

This project was originally scaffolded from:

`luchonaveiro/open-source-data-stack`

The upstream repository demonstrates an open-source data stack using technologies such as Airflow, PostgreSQL, dbt, Great Expectations, and Superset.

This project uses that repository as structural inspiration but replaces and extends the original implementation with its own:

- Chicago Taxi Trips data source;
- SODA 3 API ingestion logic;
- PostgreSQL schema;
- raw ingestion model;
- dimensional dbt models;
- data-quality tests;
- Airflow workflow;
- retry / backfill / failure-recovery experiments;
- GitHub Actions CI;
- PySpark batch-processing job;
- Parquet partition experiments;
- Spark resource observations;
- project-specific architecture and documentation.

The upstream repository is used as a scaffold rather than as the finished project implementation.