from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import datetime


from postgres.install_gold import install_gold
from postgres.load_gold import load_gold_table

with DAG(
    dag_id="chicago_taxi_gold_postgres",
    description="install postgres gold schema and load gold tables from S3",
    schedule=None,  # declenchement manuel pour ce test technique
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["data-engineering", "spark", "exposition-layer"],
) as dag:

    install_gold_task = PythonOperator(
        task_id="install_gold",
        python_callable=install_gold,
    )

    load_daily_revenue = PythonOperator(
        task_id="load_daily_revenue",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "daily_revenue"},
    )

    load_daily_average_trip_duration = PythonOperator(
        task_id="load_daily_average_trip_duration",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "daily_average_trip_duration"},
    )

    load_taxi_daily_performance = PythonOperator(
        task_id="load_taxi_daily_performance",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "taxi_daily_performance"},
    )

    load_company_daily_performance = PythonOperator(
        task_id="load_company_daily_performance",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "company_daily_performance"},
    )

    load_taxi_monthly_performance = PythonOperator(
        task_id="load_taxi_monthly_performance",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "taxi_monthly_performance"},
    )

    load_company_monthly_performance = PythonOperator(
        task_id="load_company_monthly_performance",
        python_callable=load_gold_table,
        op_kwargs={"table_name": "company_monthly_performance"},
    )

    install_gold_task >> [
        load_daily_revenue,
        load_daily_average_trip_duration,
        load_taxi_daily_performance,
        load_company_daily_performance,
        load_taxi_monthly_performance,
        load_company_monthly_performance,
    ]