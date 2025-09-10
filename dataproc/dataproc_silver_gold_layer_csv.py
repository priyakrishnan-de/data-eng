from pyspark.sql import SparkSession
from datetime import datetime

def run_query(spark, jdbc_url, properties, query, table_name, gcs_bucket):
    """
    Execute a SQL query on Postgres via JDBC.
    For CREATE TABLE AS SELECT, we push down query via JDBC connection.
    """
    print(f"Running query for {table_name} ...")

    # Spark cannot run DDL directly, so we use JDBC connection through spark.read
    # Trick: run query as subselect -> then write as new table
    df = spark.read.jdbc(url=jdbc_url, table=f"({query}) as subq", properties=properties)

    # overwrite the table
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)

    print(f"✅ Table {table_name} created successfully.")

    # Export as CSV into GCS
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    gcs_path = f"gs://{gcs_bucket}/{table_name}_{timestamp}.csv"

     # Write CSV with header, coalesce to 1 file
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(gcs_path)

    print(f"📂 Exported {table_name} data to GCS: {gcs_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldLayerTables").getOrCreate()

    # JDBC connection details
    jdbc_url = "jdbc:postgresql://10.25.192.3:5432/postgres"
    connection_properties = {
        "user": "postgres",
        "password": "Bestofme24!",
        "driver": "org.postgresql.Driver"
    }

    # GCS bucket for CSV exports
    gcs_bucket = "avito-exported-data-delta/gold_layer"

    # --- A1. CTR by Ad type, Category, Region ---
    query_ctr_ad_perf = """
        SELECT
            c.day,
            c."AdID",
            c."UserID",
            c.impressions,
            c.clicks,
            c.ctr,
            COALESCE(pr.phone_requests, 0) AS phone_requests
        FROM (
            SELECT
                ts."AdID",
                ts."UserID",
                ts."SearchDate"::date AS day,
                COUNT(*) AS impressions,
                COUNT(*) FILTER (WHERE ts."IsClick" = 1) AS clicks,
                (COUNT(*) FILTER (WHERE ts."IsClick" = 1)::float / NULLIF(COUNT(*), 0)) AS ctr
            FROM "TrainSearchstream_Silver" ts
            GROUP BY ts."AdID", ts."UserID", day
        ) c
        LEFT JOIN (
            SELECT
                pr."AdID",
                pr."UserID",
                pr."PhoneRequestDate"::date AS day,
                COUNT(*) AS phone_requests
            FROM "PhoneRequestsStream" pr
            GROUP BY pr."AdID", pr."UserID", day
        ) pr
        ON c."AdID" = pr."AdID" AND c."UserID" = pr."UserID" AND c.day = pr.day
        ORDER BY c.day, c."AdID", c."UserID"
    """
    run_query(spark, jdbc_url, connection_properties, query_ctr_ad_perf, "gold_ctr_ad_perf",gcs_bucket)

    # --- A2. Top Ads by clicks and conversions ---
    query_top_ads = """
        SELECT
            ts."AdID",
            ts."Title",
            ts."Ad_Type",
            COUNT(ts."AdID") AS impressions,
            SUM(CASE WHEN ts."IsClick" = 1 THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN ts."IsClick" = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts."AdID"),0), 4) AS ctr,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts."AdID"),0), 4) AS conversion_rate
        FROM "TrainSearchstream_Silver" ts
        LEFT JOIN "PhoneRequestsStream" pr ON ts."UserID" = pr."UserID" AND ts."AdID" = pr."AdID"
        LEFT JOIN "VisitsStream" vs ON ts."UserID" = vs."UserID"
        GROUP BY ts."AdID", ts."Title", ts."Ad_Type"
        ORDER BY clicks DESC
    """
    run_query(spark, jdbc_url, connection_properties, query_top_ads, "gold_top_ads",gcs_bucket)

    # --- A3. Revenue Contribution by ad type ---
    query_adtype_revenue = """
      SELECT
            ts."AdID",
            ts."Title",
            ts."Ad_Type",
            COUNT(ts."AdID") AS impressions,
            SUM(CASE WHEN ts."IsClick" = 1 THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN ts."IsClick" = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts."AdID"),0), 4) AS ctr,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts."AdID"),0), 4) AS conversion_rate
        FROM "TrainSearchstream_Silver" ts
        LEFT JOIN "PhoneRequestsStream" pr ON ts."UserID" = pr."UserID" AND ts."AdID" = pr."AdID"
        LEFT JOIN "VisitsStream" vs ON ts."UserID" = vs."UserID"
        GROUP BY ts."AdID", ts."Title", ts."Ad_Type"
        ORDER BY clicks DESC
    """
    run_query(spark, jdbc_url, connection_properties, query_adtype_revenue, "gold_adtype_revenue",gcs_bucket)

    spark.stop()