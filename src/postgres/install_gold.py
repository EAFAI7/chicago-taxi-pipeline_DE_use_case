import os
from pathlib import Path

import psycopg2


def install_gold():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_BI_HOST", "postgres_bi"),
        port=os.getenv("POSTGRES_BI_PORT", "5432"),
        dbname=os.getenv("POSTGRES_BI_DB", "analytics"),
        user=os.getenv("POSTGRES_BI_USER", "analytics"),
        password=os.getenv("POSTGRES_BI_PASSWORD", "analytics"),
    )

    cursor = conn.cursor()

    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    ddl_dir = Path("/opt/airflow/sql/gold")

    for sql_file in sorted(ddl_dir.glob("*.sql")):
        print(f"[postgres] Installing {sql_file.name}")
        cursor.execute(sql_file.read_text(encoding="utf-8"))

    conn.commit()

    cursor.close()
    conn.close()

    print("[postgres] Gold schema installed successfully.")