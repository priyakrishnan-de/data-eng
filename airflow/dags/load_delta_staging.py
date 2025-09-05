
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_delta_to_staging_and_transform():
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

    #Ensureing staging marker table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS staging_extract_marker (
        id SERIAL PRIMARY KEY,
        last_max_id BIGINT DEFAULT 0,
        insert_count INTEGER DEFAULT 0
        )
    """)

    #create the staging and marker tables if they do not exist irrespective of other queries
    conn.commit()

    # Get last max ID from last run marked in extract_marker table
    cur.execute("SELECT COALESCE(MAX(last_max_id), 0) FROM staging_extract_marker")
    result = cur.fetchone()
    
    #check if last_max_id is 0, then this is the first run - manually assign initial load value
    last_max_id = result[0] if result and result[0] else 15961515
    
    print(f"last max id is: {last_max_id}")

    # Fetch new records
    cur.execute("""
        SELECT id, searchid, adid, position, objecttype, histctr, isclick
        FROM trainsearchstream
        WHERE id > %s
        ORDER BY id ASC
    """, (last_max_id,))

    rows = cur.fetchall()

    if rows:
        #convert to dataframe for transformations
        df = pd.DataFrame(rows,columns=["id","searchid","adid","position","objecttype","histctr","isclick"])
        
        #--------Basic Transformations--------------
        # 1. De-duplication (by id)
        df = df.drop_duplicates(subset=['id'])

        # 2. Null defaults
        #df["isclick"] = df["isclick"].fillna(0) # default 0
        df['histctr'] = df['histctr'].fillna(0.0) #assume default 0.0 for double precision

        # 3. Type standardization
        df['id'] = df['id'].astype('int64')
        df['searchid'] = df['searchid'].astype('int64')
        df['adid'] = df['adid'].astype('int64')
        df['position'] = df['position'].astype(int)
        df['objecttype'] = df['objecttype'].astype(int)
        df['histctr'] = df['histctr'].astype(float)
        df['isclick'] = df['isclick'].fillna(0).astype(int)

        # Insert into staging table in a single transaction
        for i,row in df.iterrows():
            cur.execute("""
                INSERT INTO trainsearchstream_staging (id, searchid, adid, position, objecttype, histctr, isclick)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, tuple(row)
            )
                        
        new_max_id = df['id'].max()

        cur.execute("""
            INSERT INTO staging_extract_marker (last_max_id, insert_count) 
            VALUES (%s,%s)
            """, (int(new_max_id),len(df))
        )

        #commit both staging records and staging marker table at same time
        conn.commit()

    cur.close()
    conn.close()

    print(f"Inserted {len(df)} new records into staging and staging last processed id is {new_max_id}")

    
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 28),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "Load_delta_to_staging_and_transform",
    default_args=default_args,
    description="Load delta into bronze layer/staging with basic transformation",
    schedule_interval="*/30 * * * *",  # every 30 minutes
    catchup=False,
    tags=["avito-context","bronze", "staging"],
) as dag:

    load_task = PythonOperator(
        task_id="load_delta_to_staging_with_minimal_transform",
        python_callable=load_delta_to_staging_and_transform
    )
