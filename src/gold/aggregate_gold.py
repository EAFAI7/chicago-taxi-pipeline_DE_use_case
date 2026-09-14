"""
Couche GOLD : les KPIs finaux, calcules depuis silver et ecrits sur MinIO
en Parquet + CSV.

6 tables : 
daily_revenue, 
avg_trip_duration, 
taxi_daily_performance, 
company_daily_performance,
taxi_monthly_performance, 
company_monthly_performance. 
P.S. :Pas de base SQL à part, on lit directement les fichiers (DuckDB, pandas, Superset...
marchent tous avec du S3).
"""
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))
from common.spark_session import get_spark  # noqa: E402
from common.storage import add_year_month_column, s3a_path  # noqa: E402

def _write(df, gold_root: str, name: str):
    parquet_path = f"{gold_root}/{name}"
    csv_path = f"{gold_root}/{name}_csv"
    df.write.mode("overwrite").parquet(parquet_path)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)
    print(f"[gold] table '{name}' ecrite dans {parquet_path} (+ CSV)")


def run(silver_path: str, gold_root: str):
    spark = get_spark("gold-aggregate")
    df = spark.read.parquet(silver_path)
    
    df.cache()

    # 1. Revenus par jour
    daily_revenue = (
        df.groupBy("trip_date")
        .agg(
            F.count("trip_id").alias("nb_trips"),
            F.round(F.sum("trip_total"), 2).alias("total_daily_revenue"),
            F.round(F.sum("fare"), 2).alias("total_fare"),
            F.round(F.sum("tips"), 2).alias("total_daily_tips"),
            F.round(F.sum("tolls"), 2).alias("total_daily_tolls"),
            F.round(F.sum("extras"), 2).alias("total_daily_extras")
        )
        .orderBy("trip_date")
    )
    daily_revenue = add_year_month_column(daily_revenue, "trip_date")
    _write(daily_revenue, gold_root, "daily_revenue")

    # 2. Duree moyenne des trajets par jour
    avg_trip_duration = (
        df.groupBy("trip_date")
        .agg(
            F.round(F.avg("trip_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("trip_km"), 2).alias("avg_distance_km"),
            F.count("trip_id").alias("nb_trips"),
        )
        .orderBy("trip_date")
    )
    avg_trip_duration = add_year_month_column(avg_trip_duration, "trip_date")
    _write(avg_trip_duration, gold_root, "daily_average_trip_duration")

    # 3. Performance quotidienne par taxi : trajets, revenu, duree/distance
    # moyenne par taxi et par jour. Permet de reperer les taxis les plus
    # actifs/rentables, ou au contraire sous-utilises.
    taxi_daily_performance = (
        df.filter(F.col("taxi_id").isNotNull())
        .groupBy("taxi_id", "trip_date")
        .agg(
            F.count("trip_id").alias("nb_trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.sum("trip_minutes"), 2).alias("total_duration_minutes"),
            F.round(F.sum("trip_km"), 2).alias("total_distance_km"),
        )
        .orderBy("trip_date", "taxi_id")
    )
    taxi_daily_performance = add_year_month_column(taxi_daily_performance, "trip_date")
    _write(taxi_daily_performance, gold_root, "taxi_daily_performance")

    # 4. Performance quotidienne par compagnie : meme logique que pour les
    # taxis, mais au niveau de la compagnie. Utile pour comparer les
    # compagnies entre elles (volume, revenu, temps/distance moyens).
    company_daily_performance = (
        df.filter(F.col("company").isNotNull())
        .groupBy("company", "trip_date")
        .agg(
            F.count("trip_id").alias("nb_trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.sum("trip_minutes"), 2).alias("total_duration_minutes"),
            F.round(F.sum("trip_km"), 2).alias("total_distance_km"),
        )
        .orderBy("trip_date", "company")
    )
    company_daily_performance = add_year_month_column(company_daily_performance, "trip_date")
    _write(company_daily_performance, gold_root, "company_daily_performance")

    # 5. Performance mensuelle par taxi : meme metriques que le quotidien,
    # agregees au mois (year_month est deja present sur df, herite du
    # partitionnement bronze/silver, pas besoin de le recalculer).
    taxi_monthly_performance = (
        df.filter(F.col("taxi_id").isNotNull())
        .groupBy("taxi_id", "year_month")
        .agg(
            F.count("trip_id").alias("nb_trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.sum("trip_minutes"), 2).alias("total_duration_minutes"),
            F.round(F.sum("trip_km"), 2).alias("total_distance_km"),
        )
        .orderBy("year_month", "taxi_id")
    )
    _write(taxi_monthly_performance, gold_root, "taxi_monthly_performance")

    # 6. Performance mensuelle par compagnie.
    company_monthly_performance = (
        df.filter(F.col("company").isNotNull())
        .groupBy("company", "year_month")
        .agg(
            F.count("trip_id").alias("nb_trips"),
            F.round(F.sum("trip_total"), 2).alias("total_revenue"),
            F.round(F.sum("trip_minutes"), 2).alias("total_duration_minutes"),
            F.round(F.sum("trip_km"), 2).alias("total_distance_km"),
        )
        .orderBy("year_month", "company")
    )
    _write(company_monthly_performance, gold_root, "company_monthly_performance")

    df.unpersist()
    spark.stop()


def main():
    silver_path = s3a_path("silver", "trips")
    gold_root = s3a_path("gold")

    run(silver_path, gold_root)


if __name__ == "__main__":
    main()