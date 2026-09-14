"""
DAG orchestrant le pipeline Chicago Taxi Trips : download -> bronze -> silver -> gold.

Chaque tâche execute une fonction Python qui lance elle-même un job Spark
(local[*]). Le PYTHONPATH est configuré (voir Dockerfile) pour que le
package `src` soit importable depuis le container Airflow.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _download():
    from ingestion.download_chicago_taxi import main as download_main
    download_main()


def _bronze():
    from bronze.ingest_bronze import main as bronze_main
    bronze_main()


def _silver():
    from silver.clean_silver import main as silver_main
    silver_main()


def _gold():
    from gold.aggregate_gold import main as gold_main
    gold_main()


default_args = {
    "owner": "data-eng",
    "retries": 1,
}

with DAG(
    dag_id="chicago_taxi_medallion_pipeline",
    description="Ingestion -> Bronze -> Silver -> Gold pour Chicago Taxi Trips",
    default_args=default_args,
    schedule=None,  # declenchement manuel pour ce test technique
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["data-engineering", "spark", "medallion"],
) as dag:

    download_raw = PythonOperator(
        task_id="download_raw_data",
        python_callable=_download,
    )

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=_bronze,
    )

    clean_silver = PythonOperator(
        task_id="clean_silver",
        python_callable=_silver,
    )

    aggregate_gold = PythonOperator(
        task_id="aggregate_gold",
        python_callable=_gold,
    )

    download_raw >> ingest_bronze >> clean_silver >> aggregate_gold
