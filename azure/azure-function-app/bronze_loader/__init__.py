import logging
import pg8000
import pandas as pd
import os
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing delta load request...")

    # PostgreSQL connection details from environment variables (configure in Azure Function App settings)
    pg_host = os.getenv("PG_HOST")
    pg_db = os.getenv("PG_DATABASE")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASSWORD")
    pg_port = os.getenv("PG_PORT", "5432")

    try:
        conn = pg8000.connect(
            host=pg_host,
            database=pg_db,
            user=pg_user,
            password=pg_pass,
            port=pg_port
        )
        cur = conn.cursor()

        # Ensure staging table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS "TrainSearchStream_Staging" (
                "ID" BIGINT PRIMARY KEY,
                "SearchID" BIGINT,
                "AdID" BIGINT,
                "Position" INTEGER,
                "ObjectType" INTEGER,
                "HistCTR" DOUBLE PRECISION,
                "IsClick" INTEGER
            )
        """)

        # Ensure marker table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS staging_extract_marker (
                id SERIAL PRIMARY KEY,
                last_max_id BIGINT DEFAULT 0,
                insert_count INTEGER DEFAULT 0
            )
        """)
        conn.commit()

        # Get last max ID from marker table
        cur.execute("SELECT COALESCE(MAX(last_max_id), 0) FROM staging_extract_marker")
        result = cur.fetchone()
        last_max_id = result[0] if result and result[0] else 15961515

        logging.info(f"Last max id is: {last_max_id}")

        # Fetch new records
        cur.execute("""
            SELECT "ID", "SearchID", "AdID", "Position", "ObjectType", "HistCTR", "IsClick"
            FROM "TrainSearchStream"
            WHERE "ID" > %s
            ORDER BY "ID" ASC
        """, (last_max_id,))
        rows = cur.fetchall()

        insert_count = 0
        new_max_id = last_max_id

        if rows:
            df = pd.DataFrame(rows, columns=["id","searchid","adid","position","objecttype","histctr","isclick"])

            # Transformations
            df = df.drop_duplicates(subset=['id'])
            
            #df['id'] = df['id'].fillna(0).astype(int)
            #df['histctr'] = df['histctr'].fillna(0.0)
            #df['isclick'] = df['isclick'].fillna(0).astype(int)

            # Fill NaNs and convert types before inserting
            int_cols = ['id', 'searchid', 'adid', 'position', 'objecttype', 'isclick']
            for col in int_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

            df['histctr'] = pd.to_numeric(df['histctr'], errors='coerce').fillna(0.0).astype(float)


            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO "TrainSearchStream_Staging"
                    ("ID", "SearchID", "AdID", "Position", "ObjectType", "HistCTR", "IsClick")
                    VALUES (
                        COALESCE(%s, 0),
                        COALESCE(%s, 0),
                        COALESCE(%s, 0),
                        COALESCE(%s, 0),
                        COALESCE(%s, 0),
                        COALESCE(%s, 0.0),
                        COALESCE(%s, 0)
                    )
                    ON CONFLICT ("ID") DO NOTHING
                """, (
                    int(row['id']),
                    int(row['searchid']),
                    int(row['adid']),
                    int(row['position']),
                    int(row['objecttype']),
                    float(row['histctr']),
                    0 if pd.isna(row['isclick']) else int(float(row['isclick']))
                ))

            new_max_id = df['id'].max()
            insert_count = len(df)

            cur.execute("""
                INSERT INTO staging_extract_marker (last_max_id, insert_count)
                VALUES (%s, %s)
            """, (int(new_max_id), insert_count))
            
            conn.commit()

        cur.close()
        conn.close()

        msg = f"Inserted {insert_count} new records. Last processed id: {new_max_id}"
        logging.info(msg)
        return func.HttpResponse(msg, status_code=200)

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
