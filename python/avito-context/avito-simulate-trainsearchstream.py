
# This script simulates the creation of the TrainSearchStream table which has additional column Isclick by reading from TestSearchStream and inserting data into TrainSearchStream in batches.
# IsClick is set to NULL for ObjectType 3, otherwise randomly set to 1 with 5% probability and 0 with 95% probability.

import psycopg2
import random
import io

# PostgreSQL connection
conn = psycopg2.connect(
    dbname="avito-context",
    user="postgres",
    password="test123",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Step 1: Create trainsearchstream if not exists
create_sql = """
CREATE TABLE IF NOT EXISTS TrainSearchStream (
    ID BIGINT,
    SearchID BIGINT,
    AdID BIGINT,
    Position BIGINT,
    ObjectType BIGINT,
    HistCTR DOUBLE PRECISION,
    IsClick INT
);
"""
cur.execute(create_sql)
conn.commit()

print("✅ Ensured trainsearchstream table exists")

def sql_val(v):
    return r"\N" if v is None else str(v)


batch_size = 100000
rows_buffer = io.StringIO()


# Step 2: Use server-side cursor for streaming (doesn't load all rows in memory)

read_conn = psycopg2.connect(
    dbname="avito-context",
    user="postgres",
    password="test123",
    host="localhost",
    port="5432"
)
read_cur = read_conn.cursor("stream_cursor")  # server-side cursor
read_cur.itersize = 100000
read_cur.execute("SELECT ID, SearchID, AdID, Position, ObjectType, HistCTR FROM TestSearchStream")

def flush_buffer():
    global rows_buffer, count
    rows_buffer.seek(0)
    cur.copy_from(rows_buffer, "trainsearchstream", sep=",", null="\\N",columns=("id","searchid","adid","position","objecttype","histctr","isclick"))
    conn.commit()
    rows_buffer.close()
    rows_buffer = io.StringIO()
    print(f"Inserted {count:,} rows so far...")

count = 0

print("⏳ Starting to insert rows into trainsearchstream...")


for row in read_cur:
    ID, SearchID, AdID, Position, ObjectType, HistCTR = row
    ID = sql_val(ID)
    SearchID = sql_val(SearchID)
    AdID = sql_val(AdID)
    Position = sql_val(Position)
    ObjectType = sql_val(ObjectType)
    HistCTR = sql_val(HistCTR)

# Handle NULLs properly
HistCTR_val = "NULL" if HistCTR is None else HistCTR

if ObjectType == 3:
    IsClick = r"\N"
else:
    IsClick = "1" if random.random() < 0.05 else "0"

rows_buffer.write(f"{ID},{SearchID},{AdID},{Position},{ObjectType},{HistCTR},{IsClick}\n")
count += 1

if count % batch_size == 0:
    flush_buffer()
    print(f"{count} rows copied...")

# Flush remaining
if rows_buffer.tell() > 0:
    flush_buffer()

read_cur.close()
cur.close()
conn.close()
print(f"✅ Finished inserting {count:,} rows into trainsearchstream")
