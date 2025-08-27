
#This scripts ingests 10 files from Avito Context Ad Clicks competition into a PostgreSQL database

import pandas as pd
from sqlalchemy import create_engine

# Database connection details
db_host = "localhost"
db_name = "avito-context"
db_user = "postgres"
db_password = "test123"
db_port = "5432"

db_connection_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
print(db_connection_str)

# CSV file path and table name
csv_file_path = "C:/Users/OrCon/Documents/datasets/avito-context-ad-clicks/"
#table_name = "category"
    
# Connect to PostgreSQL
engine = create_engine(db_connection_str)
#print(engine.connect())

# File ↔ Table mapping

"""
Batch 1 - small tsv data files
files_to_tables = {
    "UserInfo.tsv": "UserInfo",
    "Category.tsv": "Category",
    "Location.tsv": "Location",
}

# Load each file into corresponding table
for file, table in files_to_tables.items():
    print(f"Loading {file} → {table}...")
    df = pd.read_csv(csv_file_path + file, sep="\t")
    df.to_sql(table, engine, if_exists="replace", index=False, schema="public")
    print(f"Inserted {len(df)} rows into {table}")  

"""
"""

#Batch 2 - large TSV files - fastest option
files_to_tables = {
    "VisitsStream.tsv": "VisitsStream",
    "testSearchStream.tsv": "TestSearchStream",
    "SearchInfo.tsv": "SearchInfo",
    "PhoneRequestsStream.tsv": "PhoneRequestsStream",
    "AdsInfo.tsv": "AdsInfo",
}
conn = engine.raw_connection()

for file, table in files_to_tables.items():
    print(f"Loading {file} → {table}...")
    tsv_file_path = csv_file_path + file
    df = pd.read_csv(tsv_file_path, sep="\t")

    # Create table if not exists (you might want to adjust the schema as needed)
    df.head(0).to_sql(table, engine, if_exists="replace", index=False, schema="public")
    
    with conn.cursor() as cur:
        with open(tsv_file_path, "r", encoding="utf-8") as f:
            cur.copy_expert(f'COPY "{table}" FROM STDIN WITH CSV DELIMITER E\'\t\' HEADER', f)
        conn.commit()
    print(f"Data from {csv_file_path} has been ingested into the {table} table.")
"""
"""
#Batch 3 - large CSV files - fastest option
files_to_tables = {
    "sampleSubmission.csv": "SampleSubmission",
    "sampleSubmission_HistCTR.csv": "SampleSubmission_HistCTR",   
}

conn = engine.raw_connection()

for file, table in files_to_tables.items():
    print(f"Loading {file} → {table}...")
    file_path = csv_file_path + file
    df = pd.read_csv(file_path, sep=",")

    # Create table if not exists (you might want to adjust the schema as needed)
    df.head(0).to_sql(table, engine, if_exists="replace", index=False, schema="public")
    
    with conn.cursor() as cur:
        with open(file_path, "r", encoding="utf-8") as f:
            cur.copy_expert(f'COPY "{table}" FROM STDIN WITH CSV HEADER', f)
        conn.commit()
    print(f"Data from {csv_file_path} has been ingested into the {table} table.")

"""

#df = pd.read_csv(csv_file_path, sep="\t")
#print(df.head)
    
#df.to_sql(table_name, engine, if_exists='replace', index=False) # Use 'append' to add data to existing table
#print(f"Data from {csv_file_path} has been ingested into the {table_name} table.")

# Clean up
if 'engine' in locals():
    engine.dispose()    

