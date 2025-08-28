
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import psycopg2

def test_postgres_connection_direct(**kwargs):
    # Connection parameters (replace with your values)
    host = "host.docker.internal"  # Use this if using airflow on Docker container and postgres outside docker
    port = "5432"
    user = "postgres"
    password = "test123"
    database = "avito-context" # avito data is inserted here
    #database = "postgres" # this database does not have any table in local

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            sslmode="disable"
        )
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        result = cur.fetchone()
        print(f"Connection successful. Server time: {result[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")
        raise

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='postgres_local_directtest',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # manual trigger
    catchup=False,
) as dag:

    test_conn_task = PythonOperator(
        task_id='test_postgres_direct_task',
        python_callable=test_postgres_connection_direct
    )

    test_conn_task
