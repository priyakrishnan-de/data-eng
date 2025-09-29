
from __future__ import annotations

import tempfile  # Import the tempfile module
import io
import pandas as pd
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
import datetime
import os
import csv

# ====================================================================
# Airflow Variables and Connections
# ====================================================================
# This is the connection ID you must create in the Airflow UI.
# It should contain the private IP of your Cloud SQL instance.
POSTGRES_CONN_ID = "cloudsql_postgres_conn"

# Replace this with the name of your GCS bucket
GCS_BUCKET_NAME = "avito-raw"


with DAG(
    dag_id="avito_data_ingestion_pipeline",
    start_date=datetime.datetime(2025, 9, 3),
    schedule_interval=None,
    catchup=False,
    tags=["data-ingestion", "postgres", "avito"],
) as dag:
    
    # ====================================================================
    # Helper Function to get connection details
    # This function is used to securely retrieve database connection
    # parameters from the Airflow UI without hardcoding them.
    # ====================================================================
    def get_db_connection_details(conn_id):
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_uri().replace("postgresql", "postgresql+psycopg2")

    
    # ====================================================================
    # Task to ingest large TSV files using copy_expert
    # This is the fastest and most efficient method for large files.
    # We use a raw connection to leverage PostgreSQL's COPY command.
    # ====================================================================
    
    @task(task_id="ingest_large_tsv_files_with_copy")
    def ingest_large_tsv_files_with_copy():
        import logging
        gcs_hook = GCSHook()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        files_to_tables = {
            #"Category.tsv": "Category",
            #"Location.tsv": "Location",
            #"UserInfo.tsv": "UserInfo",
            #"testSearchStream.tsv": "TestSearchStream",
            #"SearchInfo.tsv": "SearchInfo",
            #"AdsInfo.tsv": "AdsInfo",
            #"PhoneRequestsStream.tsv": "PhoneRequestsStream",
            "VisitsStream.tsv": "VisitsStream"
        }

        for file, table in files_to_tables.items():
            logging.info(f"Loading {file} -> {table}...")

            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as temp_file:
                gcs_hook.download(
                    bucket_name=GCS_BUCKET_NAME,
                    object_name=file,
                    filename=temp_file.name
                )

            # --- Build schema only if table is new ---
            conn = pg_hook.get_conn()   # psycopg2 connection
            
            #engine = pg_hook.get_sqlalchemy_engine()

            #with open(temp_file.name, mode="r") as f_header:
            #    df = pd.read_csv(f_header, sep="\t", nrows=0)
            #    df.to_sql(table.lower(), engine,if_exists="append",index=False,schema="public")

            # --- Bulk load ---            
            cur = conn.cursor()
            with open(temp_file.name, mode="r") as f_data:
                cur.copy_expert(
                    f'COPY "{table}" FROM STDIN WITH CSV DELIMITER E\'\t\' HEADER',
                    f_data
                )
                conn.commit()

            cur.close()
            conn.close()

            os.remove(temp_file.name)
            logging.info(f"✅ {file} ingested into {table}")

        return "Ingestion complete."


    large_tsv = ingest_large_tsv_files_with_copy()
    large_tsv
    