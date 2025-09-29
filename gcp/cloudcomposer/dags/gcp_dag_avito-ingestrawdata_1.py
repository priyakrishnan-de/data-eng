from __future__ import annotations

import io  # Import the io module for StringIO
import pandas as pd
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import DAG
import datetime

# ====================================================================
# Airflow Variables and Connections
# ====================================================================
# This is the connection ID you must create in the Airflow UI.
# It should contain the private IP of your Cloud SQL instance.
POSTGRES_CONN_ID = "cloudsql_postgres_conn"

# Replace this with the name of your GCS bucket
GCS_BUCKET_NAME = "avito-raw"

with DAG(
    dag_id="avito_rawdata_ingestion_pipeline",
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

    """
    # ====================================================================
    # Task to ingest small TSV files using pandas
    # This is a good method for smaller datasets that fit into memory.
    # ====================================================================
    @task(task_id="ingest_small_tsv_files")
    def ingest_small_tsv_files():
        
        #Ingests small TSV files into PostgreSQL using pandas.to_sql.
        #This method is simple but can be slow for large files.
        
        gcs_hook = GCSHook()
        db_uri = get_db_connection_details(POSTGRES_CONN_ID)
        
        files_to_tables = {
            "UserInfo.tsv": "UserInfo",
            "Category.tsv": "Category",
            "Location.tsv": "Location",
        }
        
        for file, table in files_to_tables.items():
            print(f"Loading {file} -> {table}...")
            # Use gcs_hook to read the file content
            file_content = gcs_hook.download(bucket_name=GCS_BUCKET_NAME, object_name=file)
            df = pd.read_csv(io.StringIO(file_content.decode('utf-8')), sep="\t")
            
            # Write to SQL database
            df.to_sql(table, db_uri, if_exists="replace", index=False, schema="public")
            print(f"Inserted {len(df)} rows into {table}")

    """

    # ====================================================================
    # Task to ingest large TSV files using copy_expert
    # This is the fastest and most efficient method for large files.
    # We use a raw connection to leverage PostgreSQL's COPY command.
    # ====================================================================
    @task(task_id="ingest_large_tsv_files_with_copy")
    def ingest_large_tsv_files_with_copy():
        
        #Ingests large TSV files into PostgreSQL using the COPY command.
        #This is a highly optimized method for bulk ingestion.
        
        gcs_hook = GCSHook()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        files_to_tables = {
            "UserInfo.tsv": "UserInfo",
            "Category.tsv": "Category",
            "Location.tsv": "Location",
            #"AdsInfo.tsv": "AdsInfo",
            #"VisitsStream.tsv": "VisitsStream",
            #"testSearchStream.tsv": "TestSearchStream",
            #"SearchInfo.tsv": "SearchInfo",
            #"PhoneRequestsStream.tsv": "PhoneRequestsStream",
        }
        
        with pg_hook.get_conn() as conn:
            for file, table in files_to_tables.items():
                print(f"Loading {file} -> {table}...")
                
                # Fetch the file content from GCS
                tsv_content = gcs_hook.download(
                    bucket_name=GCS_BUCKET_NAME,
                    object_name=file
                )
                
                # Create a DataFrame from the first few rows to infer schema
                df = pd.read_csv(io.StringIO(tsv_content.decode('utf-8')), sep="\t", nrows=10)
                df.to_sql(table, pg_hook.get_sqlalchemy_engine(), if_exists="replace", index=False, schema="public")
                
                # Use the COPY command for bulk data loading
                with conn.cursor() as cur:
                    cur.copy_expert(f'COPY "{table}" FROM STDIN WITH CSV DELIMITER E\'\t\' HEADER', io.StringIO(tsv_content.decode('utf-8')))
                conn.commit()
                print(f"Data from {file} has been ingested into the {table} table.")
    

    """
    # ====================================================================
    # Task to ingest large CSV files using copy_expert
    # Similar to the TSV task, but with a different delimiter.
    # ====================================================================
    @task(task_id="ingest_large_csv_files_with_copy")
    def ingest_large_csv_files_with_copy():
        
        Ingests large CSV files into PostgreSQL using the COPY command.
        
        gcs_hook = GCSHook()
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        files_to_tables = {
            "sampleSubmission.csv": "SampleSubmission",
            "sampleSubmission_HistCTR.csv": "SampleSubmission_HistCTR",  
        }
        
        with pg_hook.get_conn() as conn:
            for file, table in files_to_tables.items():
                print(f"Loading {file} -> {table}...")
                
                csv_content = gcs_hook.download(
                    bucket_name=GCS_BUCKET_NAME,
                    object_name=file
                )
                
                df = pd.read_csv(io.StringIO(csv_content.decode('utf-8')), sep=",", nrows=10)
                df.to_sql(table, pg_hook.get_sqlalchemy_engine(), if_exists="replace", index=False, schema="public")
                
                with conn.cursor() as cur:
                    cur.copy_expert(f'COPY "{table}" FROM STDIN WITH CSV HEADER', io.StringIO(csv_content.decode('utf-8')))
                conn.commit()
                print(f"Data from {file} has been ingested into the {table} table.")
    """
    # ====================================================================
    # Define Task Dependencies
    # All three ingestion tasks can run in parallel since they don't depend on each other.
    # ====================================================================
    #small_tsv = ingest_small_tsv_files()
    large_tsv = ingest_large_tsv_files_with_copy()
    #large_csv = ingest_large_csv_files_with_copy()

    # Set the order of operations
    #small_tsv
    large_tsv
    #large_csv
