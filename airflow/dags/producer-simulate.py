
from datetime import datetime, timedelta
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_records():
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()

    #Get the last id and increment by 1 to keep track of the generated rows
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM trainsearchstream")
    last_id = cur.fetchone()[0]

    new_rows = []
    for i in range(1,10):  # insert ~10 rows
        next_id = last_id + i
        obj_type = random.choice([1, 2, 3])
        is_click = None
        if obj_type in [1, 2]:
            is_click = 1 if random.random() < 0.05 else 0
        
        new_rows.append((
            next_id,   # id
            random.randint(1, 1e6),   # searchid
            random.randint(1, 1e6),   # adid
            random.randint(1, 10),    # position
            obj_type,
            round(random.random(), 6),# histctr
            is_click
        ))
        for row in new_rows:
            cur.execute(
                """INSERT INTO trainsearchstream
                (id, searchid, adid, position, objecttype, histctr, isclick)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", row
            )

    conn.commit()
    cur.close()
    conn.close()
    print(f"Inserted {len(new_rows)} new records into trainsearchstream.")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 27),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "producer_simulate",
    default_args=default_args,
    schedule_interval="*/2 * * * *",  # every 2 mins
    catchup=False
) as dag:

    insert_task = PythonOperator(
        task_id="insert_new_records",
        python_callable=insert_records
    )
