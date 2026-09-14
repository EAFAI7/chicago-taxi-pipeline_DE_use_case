"""
Couche SILVER : nettoyage et normalisation des trajets bronze. Lit et écrit
sur MinIO (stockage objet).

Règles de qualité appliquées:
    - deduplication sur trip_id (garde la ligne la plus récemment ingérée)
    - trip_id, trip_start_timestamp non nuls
    - trip_seconds > 0 et < 24h (élimine les valeurs aberrantes)
    - trip_miles >= 0 et < 200 (élimine les outliers grossiers)
    - fare, tips, tolls, extras, trip_total >= 0
    - cast des colonnes numeriques (string -> double/int)
    - conversion miles en km, et secondes en minutes
"""
import os
import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.spark_session import get_spark  # noqa: E402
from common.storage import s3a_path  # noqa: E402

MAX_TRIP_SECONDS = 24 * 60 * 60 # un voyage ne peut dépasser 24 heures.
MAX_TRIP_MILES = 200 # un voyage ne peut pas dépasser une distance de 200 miles (environ 322 kilomètres). 


def run(bronze_path: str, silver_path: str) -> int:
    spark = get_spark("silver-clean")

    df = spark.read.parquet(bronze_path)

    df = (
        df.withColumn("trip_seconds", F.col("trip_seconds").cast("int"))
        .withColumn("trip_miles", F.col("trip_miles").cast("double"))
        .withColumn("fare", F.col("fare").cast("double"))
        .withColumn("tips", F.col("tips").cast("double"))
        .withColumn("tolls", F.col("tolls").cast("double"))
        .withColumn("extras", F.col("extras").cast("double"))
        .withColumn("trip_total", F.col("trip_total").cast("double"))
        .withColumn("pickup_community_area", F.col("pickup_community_area").cast("int"))
        .withColumn("dropoff_community_area", F.col("dropoff_community_area").cast("int"))
    )

    # Dedup: en cas de doublon de trip_id, on garde la ligne la plus
    # récemment ingérée.
    window = Window.partitionBy("trip_id").orderBy(F.col("ingestion_timestamp").desc())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = (
        df.filter(
        F.col("trip_id").isNotNull()
        & F.col("trip_start_timestamp").isNotNull()
        & F.col("trip_seconds").isNotNull()
        & (F.col("trip_seconds") > 0)
        & (F.col("trip_seconds") < MAX_TRIP_SECONDS)
        & F.col("trip_miles").isNotNull()
        & (F.col("trip_miles") >= 0)
        & (F.col("trip_miles") < MAX_TRIP_MILES)
        & (F.coalesce(F.col("fare"), F.lit(0.0)) >= 0)
        & (F.coalesce(F.col("tips"), F.lit(0.0)) >= 0)
        & (F.coalesce(F.col("trip_total"), F.lit(0.0)) >= 0)
    )
    .withColumn("trip_km", F.round(F.col("trip_miles") * 1.609344, 3).alias("trip_distance_km"))
    .withColumn("trip_minutes", F.round(F.col("trip_seconds") / 60, 2))
    .drop("trip_miles")
    .drop("trip_seconds")
    )

    (
        df.write.mode("overwrite")
        .partitionBy("year_month")
        .parquet(silver_path)
    )

    count = df.count()
    print(f"[silver] {count} lignes ecrites dans {silver_path}")
    spark.stop()
    return count


def main():
    bronze_path = s3a_path("bronze", "trips")
    silver_path = s3a_path("silver", "trips")

    n = run(bronze_path, silver_path)
    if n == 0:
        raise RuntimeError("Aucune ligne en silver : les filtres qualité ont tout éliminé ?!")


if __name__ == "__main__":
    main()
