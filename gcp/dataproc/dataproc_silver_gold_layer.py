import os
from pyspark.sql import SparkSession

def run_query(spark, jdbc_url, properties, query, table_name):
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


if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoldLayerTables").getOrCreate()

    DB_HOST =     os.environ.get('DB_HOST', '10.25.192.3')
    DB_NAME =     os.environ.get('DB_NAME', 'postgres')
    DB_USER =     os.environ.get('DB_USER', 'postgres') 
    DB_PASSWORD = os.environ.get('DB_PASSWORD', 'Bestofme24!')
    DB_PORT =     os.environ.get('DB_PORT', '5432')

    # JDBC connection details
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

    connection_properties = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

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
    run_query(spark, jdbc_url, connection_properties, query_ctr_ad_perf, "gold_ctr_ad_perf")

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
    run_query(spark, jdbc_url, connection_properties, query_top_ads, "gold_top_ads")

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
    run_query(spark, jdbc_url, connection_properties, query_adtype_revenue, "gold_adtype_revenue")

    spark.stop()