import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SOURCE_TABLE = "raw.taxi_trips"
OUTPUT_PATH = "/app/data/parquet/taxi_trips_by_date"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("chicago-taxi-batch")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def read_raw_taxi_data(spark: SparkSession) -> DataFrame:
    postgres_host = os.environ["POSTGRES_HOST"]
    postgres_port = os.environ["POSTGRES_PORT"]
    postgres_db = os.environ["POSTGRES_DB"]
    postgres_user = os.environ["POSTGRES_USER"]
    postgres_password = os.environ["POSTGRES_PASSWORD"]

    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", SOURCE_TABLE)
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def transform_taxi_data(taxi_df: DataFrame) -> DataFrame:
    return (
        taxi_df
        .select(
            "trip_id",
            "trip_start_timestamp",
            "trip_end_timestamp",
            "trip_seconds",
            "trip_miles",
            "pickup_community_area",
            "dropoff_community_area",
            "fare",
            "tips",
            "tolls",
            "extras",
            "trip_total",
            "payment_type",
            "company",
        )
        .withColumn(
            "trip_date",
            F.to_date(F.col("trip_start_timestamp")),
        )
        .withColumn(
            "trip_seconds",
            F.col("trip_seconds").cast("double"),
        )
        .withColumn(
            "trip_miles",
            F.col("trip_miles").cast("double"),
        )
        .withColumn(
            "fare",
            F.col("fare").cast("double"),
        )
        .withColumn(
            "tips",
            F.col("tips").cast("double"),
        )
        .withColumn(
            "tolls",
            F.col("tolls").cast("double"),
        )
        .withColumn(
            "extras",
            F.col("extras").cast("double"),
        )
        .withColumn(
            "trip_total",
            F.col("trip_total").cast("double"),
        )
    )


def write_partitioned_parquet(taxi_df: DataFrame) -> None:
    (
        taxi_df
        .write
        .mode("overwrite")
        .partitionBy("trip_date")
        .parquet(OUTPUT_PATH)
    )


def validate_output(
    spark: SparkSession,
    expected_count: int,
) -> None:
    parquet_df = spark.read.parquet(OUTPUT_PATH)
    actual_count = parquet_df.count()

    if actual_count != expected_count:
        raise ValueError(
            "Parquet row count mismatch: "
            f"expected={expected_count}, actual={actual_count}"
        )

    print(
        f"Validation passed: "
        f"source_rows={expected_count}, "
        f"parquet_rows={actual_count}"
    )


def main() -> None:
    spark = create_spark_session()

    try:
        raw_df = read_raw_taxi_data(spark)
        raw_count = raw_df.count()

        print(f"Raw taxi trip count: {raw_count}")

        transformed_df = transform_taxi_data(raw_df)

        write_partitioned_parquet(transformed_df)

        print(f"Partitioned Parquet written to: {OUTPUT_PATH}")

        validate_output(
            spark=spark,
            expected_count=raw_count,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()