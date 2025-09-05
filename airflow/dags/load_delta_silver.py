import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

# DAG definition
dag = DAG(
    'Load_trainsearchstream_silver_batch',
    default_args=default_args,
    description='Silver layer ETL with batch insert',
    schedule_interval='@hourly',  # every hour
    catchup=False,
    tags=["avito-context", "silver"],
)


# Silver ETL function
def silver_etl_batch():
    hook = PostgresHook(postgres_conn_id="my_postgres_conn")
    conn = hook.get_conn()
    cur = conn.cursor()
        
    # 1. Ensuring silver table exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trainsearchstream_silver (
    id BIGINT PRIMARY KEY,
    searchid BIGINT,
    adid BIGINT,
    position INT,
    objecttype INT,
    histctr DOUBLE PRECISION,
    isclick INT,
    params TEXT,
    SearchDate TIMESTAMP,
    title TEXT,
    price DOUBLE PRECISION,
    LocationID BIGINT,
    CategoryID BIGINT,
    IPID BIGINT,
    userID BIGINT,
    searchquery TEXT,
    searchparams TEXT,
    CategoryLevel BIGINT,
    ParentCategory BIGINT,
    SubCategory BIGINT,
    Locationlevel BIGINT,
    RegionID BIGINT,
    CityID BIGINT,
    high_ctr BOOLEAN,
    ad_type TEXT);
    """)
        
    #commit creation of silver table and /or dropping earlier rows irrespective of other data updates
    cur.execute("""TRUNCATE TABLE trainsearchstream_silver;""")
    conn.commit()

    # 2. Get last max ID processed in silver
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM trainsearchstream_silver")
    last_max_id = cur.fetchone()[0]

    # 3. Fetch delta from staging and join AdsInfo in DB

    #This was the old query without join with category and location tables
    #query = """
    #    SELECT b.id, b.searchid, b.adid, b.position, b.objecttype, b.histctr, b.isclick,
    #    a.params, a.title, a.price,
    #    c.ipid, c.userid, c.searchquery, c.searchparams
    #    FROM trainsearchstream_staging b
    #    LEFT JOIN AdsInfo a ON b.adid = a.adid
    #    LEFT JOIN SearchInfo c on b.searchid = c.searchid
    #   WHERE b.id > %s
    #    ORDER BY b.id ASC
    #"""
    
    query = '''
    SELECT stg.id, stg.searchid, stg.adid, stg.position, stg.objecttype, stg.histctr, stg.isclick,
        a.params, a.title, a.price,a."LocationID", a."CategoryID",
        si.ipid, si.userid, si.searchquery, si.searchparams, si."SearchDate"::timestamp,
		c."Level" as CategoryLevel, c."ParentCategoryID" as ParentCategory, c."SubcategoryID" as SubCategory, l."Level" as Locationlevel, l."RegionID", l."CityID"
        FROM trainsearchstream_staging stg
	    LEFT JOIN AdsInfo a ON stg.adid = a.adid
        LEFT JOIN SearchInfo si on stg.searchid = si.searchid 
		LEFT JOIN "Category" c ON a."CategoryID" = c."CategoryID"
		LEFT JOIN "Location" l ON a."LocationID" = l."LocationID"
    '''

    silver_df = pd.read_sql(query, conn, params=(last_max_id,))

    if silver_df.empty:
        print("No new rows for silver layer.")
        conn.close()
        return
    
    # 4. Enrich data - Transformations
    silver_df['high_ctr'] = silver_df['histctr'] > 0.5
    silver_df['ad_type'] = silver_df['objecttype'].map({
        1: 'regular-free',
        2: 'regular-highlighted',
        3: 'contextual-payperclick'
    })
    #silver_df['isclick'] = silver_df['isclick'].fillna(0).astype(int)
    
    # 5. Insert delta into silver table
    for i, row in silver_df.iterrows():
        silver_df["ipid"] = silver_df["ipid"].fillna(0).astype("Int64")
        silver_df["userid"] = silver_df["userid"].fillna(0).astype("Int64")
        
        # Fill NaNs in text columns with empty string
        #silver_df["title"] = silver_df["title"].fillna("")

        # For numeric columns, replace NaNs with 0 (or some default)
        #silver_df["price"] = silver_df["price"].fillna(0).astype("int64")
        #print(f"row {i} is {tuple(row)}")   # shows the full tuple of values
    
        cur.execute("""
            INSERT INTO trainsearchstream_silver
            (id, searchid, adid, position, objecttype, histctr, isclick,
             params, title, price, LocationID, CategoryID, ipid, userid, searchquery, searchparams, searchdate, categorylevel, parentcategory, subcategory, locationlevel, RegionID, CityID, high_ctr, ad_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(None if pd.isna(x) else
            int(x) if isinstance(x, (np.integer)) else
            float(x) if isinstance(x, np.floating) else
            bool(x) if isinstance(x, bool) else
            x
            for x in row
            )
        )
    
    conn.commit()

    print(f"Inserted {len(silver_df)} rows into silver layer.")

    cur.close()
    conn.close()

# Airflow task
silver_task = PythonOperator(
    task_id='trainsearchstream_silver_etl_batch',
    python_callable=silver_etl_batch,
    dag=dag
)
