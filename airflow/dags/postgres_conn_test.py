
# Test connectivity to cloud SQL postgres in GCP using GCP cloud composer

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

with DAG(
    dag_id='postgres_conn_test',
    start_date=datetime(2025, 8, 25),
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'connectivity'],
) as dag:
# Task to test PostgreSQL connection by executing a simple SELECT 1 query
    test_postgres_connection = SQLExecuteQueryOperator(
        task_id='test_postgres_connection',
        conn_id='my_postgres_conn',  # Replace with your Airflow connection ID
        sql="SELECT NOW();",  # A simple query to test connection        
        autocommit=True,
        handler=lambda cursor: print(f"Connection successful. Server time: {cursor.fetchone()[0]}"),
    )
    
    test_postgres_connection


    print("DAG and task defined successfully.")