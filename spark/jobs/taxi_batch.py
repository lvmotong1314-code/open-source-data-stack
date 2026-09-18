import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("chicago-taxi-batch")
        .master("local[*]")
        .getOrCreate()
    )

    postgres_host = os.environ["POSTGRES_HOST"]
    postgres_port = os.environ["POSTGRES_PORT"]
    postgres_db = os.environ["POSTGRES_DB"]
    postgres_user = os.environ["POSTGRES_USER"]
    postgres_password = os.environ["POSTGRES_PASSWORD"]

    jdbc_url = (
        f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    )

    taxi_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw.taxi_trips")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("=== Chicago Taxi raw schema ===")
    taxi_df.printSchema()

    row_count = taxi_df.count()
    print(f"Raw taxi trip count: {row_count}")

    transformed_df = (
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

    print("=== Transformed schema ===")
    transformed_df.printSchema()

    transformed_count = transformed_df.count()
    print(f"Transformed taxi trip count: {transformed_count}")

    output_path = "/app/data/parquet/taxi_trips_unpartitioned"

    (
        transformed_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Parquet written to: {output_path}")

    parquet_df = spark.read.parquet(output_path)

    print("=== Parquet schema ===")
    parquet_df.printSchema()

    parquet_count = parquet_df.count()
    print(f"Parquet taxi trip count: {parquet_count}")

    print("=== Parquet sample ===")
    parquet_df.show(5, truncate=False)

    if transformed_count != parquet_count:
        raise ValueError(
            f"Row count mismatch: transformed={transformed_count}, parquet={parquet_count}"
        )

    spark.stop()


if __name__ == "__main__":
    main()