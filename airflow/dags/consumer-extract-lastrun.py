
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_new_records():
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    # marker table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS extract_marker (
        id SERIAL PRIMARY KEY,
        last_max_id BIGINT DEFAULT 0
    )
    """)
    conn.commit()

    # last extracted id
    cur.execute("SELECT COALESCE(MAX(last_max_id), 0) FROM extract_marker")
    last_max_id = cur.fetchone()[0]

    # fetch new data
    cur.execute("""
        SELECT * FROM trainsearchstream
        WHERE id > %s
        ORDER BY id ASC
    """, (last_max_id,))
    rows = cur.fetchall()

    if rows:
        new_max_id = max(row[0] for row in rows)
        cur.execute("INSERT INTO extract_marker (last_max_id) VALUES (%s)", (new_max_id,))
        conn.commit()

        df = pd.DataFrame(rows, columns=["id","searchid","adid","position","objecttype","histctr","isclick"])
        #df.to_csv(f"/tmp/new_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False, encoding='utf-8')
        df.to_csv("/tmp/new_records_readable.csv", index=False, encoding='utf-8')
        print(f"Extracted {len(rows)} new records, max id {new_max_id}")

    cur.close()
    conn.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 27),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "consumer_extract_lastrun",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # every 2 mins
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_records",
        python_callable=extract_new_records
    )