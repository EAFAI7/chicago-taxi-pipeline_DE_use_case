import os

from pyspark.sql import SparkSession


POSTGRES_URL = "jdbc:postgresql://postgres_bi:5432/analytics"
POSTGRES_USER = "analytics"
POSTGRES_PASSWORD = "analytics"
POSTGRES_DRIVER = "org.postgresql.Driver"

GOLD_BUCKET = "s3a://taxi-datalake/gold"


def load_gold_table(spark: SparkSession, table_name: str) -> None:
    source_path = f"{GOLD_BUCKET}/{table_name}"

    print(f"[postgres] Lecture Gold : {source_path}")

    df = spark.read.parquet(source_path)

    print(f"[postgres] {table_name}: {df.count()} lignes")

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"gold.{table_name}")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("overwrite")
        .save()
    )

    print(f"[postgres] {table_name}: chargement terminé")