
import random
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_to_staging():
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Ensure staging table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trainsearchstream_staging (
            id BIGINT PRIMARY KEY,
            searchid BIGINT,
            adid BIGINT,
            position INTEGER,
            objecttype INTEGER,
            histctr DOUBLE PRECISION,
            isclick INTEGER
        )
    """)
    conn.commit()

    # Get last max ID from last run marked in extract_marker table
    cur.execute("SELECT COALESCE(MAX(last_max_id), 0) FROM extract_marker")
    last_max_id = cur.fetchone()[0]

    # Fetch new records
    cur.execute("""
        SELECT id, searchid, adid, position, objecttype, histctr, isclick
        FROM trainsearchstream
        WHERE id > %s
        ORDER BY id ASC
    """, (last_max_id,))
    rows = cur.fetchall()

    if rows:
        
        # Insert into staging table
        for row in rows:
            
            cur.execute("""
                INSERT INTO trainsearchstream_staging (id, searchid, adid, position, objecttype, histctr, isclick)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], bool(row[6]))
            )

            new_max_id = max(row[0] for row in rows)
            cur.execute("INSERT INTO extract_marker (last_max_id) VALUES (%s)", (new_max_id,))
        
        conn.commit()
        print(f"Inserted {len(rows)} new records into staging.")

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 28),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "trainsearchstream_delta_staging",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False
) as dag:

    load_task = PythonOperator(
        task_id="load_new_records_to_staging",
        python_callable=load_to_staging
    )