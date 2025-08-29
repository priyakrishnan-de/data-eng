import os
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
    CREATE TABLE IF NOT EXISTS csv_extract_marker (
        id SERIAL PRIMARY KEY,
        last_max_id BIGINT DEFAULT 0,
        extract_count INTEGER DEFAULT 0
        )
    """)
    #create table irrespective of other queries
    conn.commit()

    # last extracted id
    cur.execute("SELECT COALESCE(MAX(last_max_id), 0) FROM csv_extract_marker")
    last_max_id = cur.fetchone()[0]
    
    #check if last_max_id is 0, then this is the first run - manually assign initial load value
    last_max_id = last_max_id if last_max_id else 15961515
    
    print(f"last max id is: {last_max_id}")

    # fetch new data
    cur.execute("""
        SELECT * FROM trainsearchstream
        WHERE id > %s
        ORDER BY id ASC
    """, (last_max_id,))
    
    rows = cur.fetchall()

    if not rows:
        print("No new rows found for CSV extraction.")
        conn.rollback()
        return

    if rows:
        os.makedirs("/opt/airflow/tmp", exist_ok=True)
        # this stores files inside docker worker container /opt/airflow/tmp. By default any folder will be created in root in same level as "opt".
        filepath = f"/opt/airflow/tmp/new_records_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
   
        df = pd.DataFrame(rows, columns=["id","searchid","adid","position","objecttype","histctr","isclick"])
           
        df.to_csv(filepath, index=False, encoding='utf-8')

        new_max_id = max(row[0] for row in rows)
        cur.execute("INSERT INTO csv_extract_marker (last_max_id, extract_count) VALUES (%s,%s)", (new_max_id,len(rows)))
        conn.commit()

        print(f"Extracted {len(rows)} new records into {filepath} and csv last processed id is {new_max_id}")

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
    schedule_interval="*/10 * * * *",  # every 10 mins
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id="extract_new_records",
        python_callable=extract_new_records
    )
