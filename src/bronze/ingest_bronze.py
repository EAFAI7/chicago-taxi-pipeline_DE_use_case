"""
Couche BRONZE : ingestion des fichiers JSON bruts (issus de l'API SODA) telle
quelle, sans logique métier. On type juste les colonnes minimalement (les
champs SODA arrivent en string) et on partitionne par mois, pour que les
couches suivantes puissent lire efficacement par période.

Aucune ligne n'est filtrée ou dedupliquée ici : c'est le rôle de la couche
Silver. Bronze doit rester une image fidèle de la source.

Le JSON brut est lu depuis le disque local (landing zone transitoire,
data/raw/ - le résultat du téléchargement API). La sortie bronze, elle, est
écrite sur MinIO (stockage objet, socle du datalake).
"""
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.spark_session import get_spark  # noqa: E402
from common.storage import add_year_month_column, s3a_path  # noqa: E402

RAW_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("taxi_id", StringType(), True),
    StructField("trip_start_timestamp", StringType(), True),
    StructField("trip_end_timestamp", StringType(), True),
    StructField("trip_seconds", StringType(), True),
    StructField("trip_miles", StringType(), True),
    StructField("pickup_community_area", StringType(), True),
    StructField("dropoff_community_area", StringType(), True),
    StructField("fare", StringType(), True),
    StructField("tips", StringType(), True),
    StructField("tolls", StringType(), True),
    StructField("extras", StringType(), True),
    StructField("trip_total", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("company", StringType(), True),
    StructField("pickup_centroid_latitude", StringType(), True),
    StructField("pickup_centroid_longitude", StringType(), True),
    StructField("dropoff_centroid_latitude", StringType(), True),
    StructField("dropoff_centroid_longitude", StringType(), True),
])


def run(raw_dir: Path, bronze_path: str) -> int:
    spark = get_spark("bronze-ingest")

    input_path = str(raw_dir / "*.json")
    df = spark.read.schema(RAW_SCHEMA).option("multiLine", True).json(input_path)

    df = df.withColumn(
        "trip_start_timestamp", F.to_timestamp("trip_start_timestamp")
    ).withColumn(
        "trip_date", F.to_date("trip_start_timestamp")
    ).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    )
    df = add_year_month_column(df, "trip_date")

    (
        df.write.mode("overwrite")
        .partitionBy("year_month")
        .parquet(bronze_path)
    )

    count = df.count()
    print(f"[bronze] {count} lignes écrites dans {bronze_path}")
    spark.stop()
    return count


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
    raw_dir = data_dir / "raw"
    bronze_path = s3a_path("bronze", "trips")

    n = run(raw_dir, bronze_path)
    if n == 0:
        raise RuntimeError("Aucune ligne en bronze : vérifier la couche raw.")


if __name__ == "__main__":
    main()
