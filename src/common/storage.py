"""
Construit des chemins s3a:// vers le bucket MinIO qui sert de stockage
objet pour les couches bronze/silver/gold (le socle du datalake, cf.
consigne du test technique).

Usage : s3a_path("bronze", "trips") -> "s3a://taxi-datalake/bronze/trips"
"""
import os


def s3a_path(*parts: str) -> str:
    bucket = os.environ.get("MINIO_BUCKET", "taxi-datalake")
    return f"s3a://{bucket}/" + "/".join(parts)


def add_year_month_column(df, date_col: str = "trip_date"):
    """Colonne year_month au format 'YYYY-MM', pour partitionner le Parquet."""
    from pyspark.sql import functions as F
    return df.withColumn("year_month", F.date_format(F.col(date_col), "yyyy-MM"))
