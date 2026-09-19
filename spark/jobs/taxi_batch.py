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

    spark.sparkContext.setLogLevel("WARN")

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


    (
        transformed_df
        .groupBy("trip_date")
        .count()
        .orderBy("trip_date")
        .show(30, truncate=False)
    )

    output_path = "/app/data/parquet/taxi_trips_unpartitioned"

    (
        transformed_df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Parquet written to: {output_path}")


    partitioned_output_path = "/app/data/parquet/taxi_trips_by_date"

    (
        transformed_df
        .write
        .mode("overwrite")
        .partitionBy("trip_date")
        .parquet(partitioned_output_path)
    )

    print(f"Date-partitioned Parquet written to: {partitioned_output_path}")



    spark.stop()


if __name__ == "__main__":
    main()