import os
import pg8000.dbapi as pg8000
import pandas as pd
from google.cloud import storage
from flask import Flask, request

app = Flask(__name__)

# ----------------------------------------------------
# Main Logic - This is the core data export function
# ----------------------------------------------------
def export_new_data_to_gcs():
    """
    Connects to Cloud SQL, reads new data from the "TrainSearchStream" table,
    and exports it to a Parquet file in a GCS bucket.
    This function uses a high-water mark to process only new records.
    """
    
    # Configuration from environment variables
    DB_USER = os.environ.get("DB_USER")
    DB_PASS = os.environ.get("DB_PASS")
    DB_NAME = os.environ.get("DB_NAME")
    DB_HOST = os.environ.get("DB_HOST")
    DB_PORT = os.environ.get("DB_PORT")
    BUCKET_NAME = os.environ.get("BUCKET_NAME")
    
    # Tables and columns
    SOURCE_TABLE = "TrainSearchStream"
    WATERMARK_TABLE = "processed_files"
    
    # Database Connection and Helper Functions
    try:
        conn = pg8000.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_NAME
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return f"Error connecting to database: {e}", 500

    def get_high_water_mark():
        cursor.execute(f'SELECT MAX(last_processed_id) FROM "{WATERMARK_TABLE}"')
        result = cursor.fetchone()[0]
        return result if result is not None else 0

    def update_high_water_mark(last_id):
        # This is a more robust way to handle the single-row high-water mark table.
        # It checks if a record exists and either updates or inserts accordingly.
        cursor.execute(f'SELECT count(*) FROM "{WATERMARK_TABLE}"')
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute(f'INSERT INTO "{WATERMARK_TABLE}" (last_processed_id) VALUES (%s)', (last_id,))
        else:
            cursor.execute(f'UPDATE "{WATERMARK_TABLE}" SET last_processed_id = %s', (last_id,))
            
        conn.commit()
    
    # Main Logic
    try:
        print("✅ Connected to the database.")
        
        last_processed_id = get_high_water_mark()
        print(f"Last processed ID: {last_processed_id}")
        
        query = f'SELECT * FROM "{SOURCE_TABLE}" WHERE "ID" > %s ORDER BY "ID" ASC;'
        cursor.execute(query, (last_processed_id,))
        records = cursor.fetchall()

        if not records:
            print("No new records to export.")
            return "No new records to export.", 200

        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(records, columns=column_names)
        
        new_high_water_mark = df['ID'].max()
        print(f"New high-water mark: {new_high_water_mark}")

        from io import BytesIO
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        filename = f"trainsearchstream_incremental_{new_high_water_mark}.parquet"
        blob = bucket.blob(filename)
        blob.upload_from_file(buffer, content_type="application/octet-stream")
        
        print(f"✅ Successfully uploaded {len(df)} records to gs://{BUCKET_NAME}/{filename}")
        
        update_high_water_mark(new_high_water_mark)
        print("✅ High-water mark updated.")
        
        return "Export successful.", 200
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return f"An error occurred: {e}", 500
    finally:
        conn.close()

# ----------------------------------------------------
# Cloud Run HTTP Endpoint
# ----------------------------------------------------
@app.route("/", methods=["POST"])
def main():
    """
    This is the entry point for the Cloud Run service.
    It triggers the data export function.
    """
    print("Cloud Run service triggered.")
    return export_new_data_to_gcs()
