
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# DAG default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    dag_id="Load_all_gold_tables_and_views",
    default_args=default_args,
    description="DAG for Gold Layer table & view creation",
    schedule_interval= "0 */2 * * *",  #every 2 hours 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["avito-context","gold"],
) as dag:

# ---START OF A. Ad Performance Analytics---------
# -------A.1. CTR by Ad type, Category, Region-------
    create_gold_ctr_ad_perf_table = PostgresOperator(
        task_id="create_gold_ctr_ad_perf",
        postgres_conn_id="my_postgres_conn",  
        sql="""
            DROP TABLE IF EXISTS gold_ctr_ad_perf;
            CREATE TABLE gold_ctr_ad_perf AS
            WITH ad_ctr AS (
            SELECT
            ts.adid,
            ts.userid,
            ts.SearchDate::date AS day,
            COUNT(*) AS impressions,
            COUNT(*) FILTER (WHERE ts.isclick = 1) AS clicks,
            (COUNT(*) FILTER (WHERE ts.isclick = 1)::float / NULLIF(COUNT(*), 0)) AS ctr
            FROM trainsearchstream_silver ts
            GROUP BY ts.adid, ts.userid, day
            ),
            phone_requests AS (
                SELECT
                pr."AdID",
                pr."UserID",
                pr."PhoneRequestDate"::date AS day,
                COUNT(*) AS phone_requests
            FROM "PhoneRequestsStream" pr
            GROUP BY pr."AdID",pr."UserID", day
            )
            SELECT
            c.day,
            c.adid,
            c.userid,
            c.impressions,
            c.clicks,
            c.ctr,
            COALESCE(pr.phone_requests, 0) AS phone_requests
            FROM ad_ctr c
            LEFT JOIN phone_requests pr
            ON c.adid = pr."AdID" AND c.userid = pr."UserID" AND c.day = pr.day
            ORDER BY c.day, c.adid, c.userid;
        """
    )
    
    # create_gold_ctr_ad_perf_table

# -------A.2 Top Ads by clicks and conversions ie clicks and phone requests-------
    create_gold_top_ads_table = PostgresOperator(
        task_id="create_gold_top_ads",
        postgres_conn_id="my_postgres_conn",  
        sql="""
            DROP TABLE IF EXISTS gold_top_ads;
            CREATE TABLE gold_top_ads AS
            SELECT
            ts.adid,
            ts.title,
            ts.ad_type,
            COUNT(ts.adid) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts.adid),0), 4) AS ctr,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts.adid),0), 4) AS conversion_rate
            FROM trainsearchstream_silver ts
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            GROUP BY ts.adid, ts.title, ts.ad_type
            ORDER BY clicks DESC;
        """
    )

    #create_gold_top_ads_table

# -------A.3. Revenue Contribution by type. Assumption: Consider as conversion generates revenue equal to price
    create_gold_adtype_revenue_table = PostgresOperator(
        task_id="create_gold_adtype_revenue",
        postgres_conn_id="my_postgres_conn",  
        sql="""
            DROP TABLE IF EXISTS gold_adtype_revenue;
            CREATE TABLE gold_adtype_revenue AS
            SELECT
            ts.adid,
            ts.title,
            ts.ad_type,
            COUNT(ts.adid) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts.adid),0), 4) AS ctr,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts.adid),0), 4) AS conversion_rate
            FROM trainsearchstream_silver ts
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            GROUP BY ts.adid, ts.title, ts.ad_type
            ORDER BY clicks DESC;
        """
    )

    #create_gold_adtype_revenue_table

#---------END OF A. Ad Performance Analytics---------

#---------START OF B. User Behavior Insights---------

# -------B.1. User Interest Profiling - Aggregate categories, locations and ad types searched by a user-------

    create_gold_user_interest_profile_view = PostgresOperator(
        task_id="create_gold_user_interest_profile",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_user_interest_profile;
            CREATE VIEW gold_user_interest_profile AS
            SELECT 
            u."UserID",
            COUNT(DISTINCT s.searchid) AS total_searches,
            COUNT(*) FILTER (WHERE s.isclick = 1) AS total_clicks,
            ARRAY_AGG(DISTINCT s.categoryid) AS preferred_categories,
            ARRAY_AGG(DISTINCT s.regionid) AS preferred_locations,
            AVG(CASE WHEN s.isclick = 1 THEN 1 ELSE 0 END)::float AS avg_ctr
            FROM trainsearchstream_silver s
            JOIN "UserInfo" u ON s.userid = u."UserID"
            GROUP BY u."UserID";
        """
    )
    #As this is a view, it needs to be run only once
    #create_gold_user_interest_profile_view

# -------B.2. Engagement metrics - Avg. impressions per search, avg. CTR per user, repeat visits-------
    create_gold_user_engagement_metrics_table = PostgresOperator(
        task_id="create_gold_user_engagement_metrics",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_user_engagement_metrics;
            CREATE TABLE gold_user_engagement_metrics AS
            WITH user_stats AS (
            SELECT
            ts.userid,
            COUNT(ts.adid) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            COUNT(DISTINCT ts.searchid) AS searches,
            COUNT(DISTINCT vs."UserID" || '-' || vs."AdID" || '-' || vs."ViewDate") AS visits
            FROM trainsearchstream_silver ts
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            GROUP BY ts.userid
            )
            SELECT
            AVG(impressions::numeric / NULLIF(searches,0)) AS avg_impressions_per_search,
            AVG(clicks::numeric / NULLIF(impressions,0)) AS avg_ctr_per_user,
            AVG(visits) AS avg_repeat_visits_per_user
            FROM user_stats;
        """
    )
    #create_gold_user_engagement_metrics_table
    
    #---------B.3. Conversion Funnel - Search → Impression → Click → Visit → Phone Request---------
    create_gold_conversion_funnel_table = PostgresOperator(
        task_id="create_gold_conversion_funnel",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_conversion_funnel;
            CREATE TABLE gold_conversion_funnel AS
            SELECT
            COUNT(DISTINCT ts.searchid) AS searches,
            COUNT(ts.adid) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            COUNT(DISTINCT vs."UserID" || '-' || vs."ViewDate") AS visits,
            COUNT(DISTINCT pr."UserID" || '-' || pr."AdID" || '-' || pr."PhoneRequestDate") AS phone_requests,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(ts.adid),0), 4) AS ctr,
            ROUND(COUNT(DISTINCT vs."UserID" || '-' || vs."ViewDate")::numeric / NULLIF(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END),0), 4) AS visit_rate_from_clicks,
            ROUND(COUNT(DISTINCT pr."UserID" || '-' || pr."AdID" || '-' || pr."PhoneRequestDate")::numeric / NULLIF(COUNT(DISTINCT vs."UserID" || '-' || vs."ViewDate"),0), 4) AS phone_request_rate_from_visits
            FROM trainsearchstream_silver ts
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID";
        """
    )
    #create_gold_conversion_funnel_table
    
    # END OF B. User Behavior Insights---------

    # START OF C. Search and Market Trends---------

    #---------C.1. Trending Categories and Locations---------

    create_gold_search_trends_table = PostgresOperator(
        task_id="create_gold_search_trends",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_search_trends;
            CREATE TABLE gold_search_trends AS
            SELECT 
            s.SearchDate::date AS day,
            s.categoryid,
            s.regionid,
            COUNT(DISTINCT s.searchid) AS total_searches,
            COUNT(*) FILTER (WHERE s.isclick = 1) AS total_clicks,
            (COUNT(*) FILTER (WHERE s.isclick = 1))::float / NULLIF(COUNT(*), 0) AS ctr
            FROM trainsearchstream_silver s
            GROUP BY s.SearchDate::date, s.categoryid, s.regionid;
        """
    )
    #create_gold_search_trends_table

    #---------C.2. Search Demand Vs Ad Supply-------------------
    # search_to_ad_ratio highlights whether demand is outpacing supply (high ratio = undersupply)---------
    
    create_gold_search_vs_supply_table = PostgresOperator(
        task_id="create_gold_search_vs_supply_table",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_search_vs_supply;
            CREATE TABLE gold_search_vs_supply AS
            WITH search_demand AS (
            SELECT
            ts.categoryid,
            COUNT(*) AS total_searches
            FROM trainsearchstream_silver ts
            GROUP BY ts.categoryid
            ),
            ad_supply AS (
            SELECT
            ads."CategoryID",
            COUNT(*) AS total_ads
            FROM AdsInfo ads
            GROUP BY ads."CategoryID"
            )
            SELECT
            sd.categoryid,
            sd.total_searches,
            COALESCE(asup.total_ads, 0) AS total_ads,
            ROUND(sd.total_searches::numeric / NULLIF(asup.total_ads, 0), 2) AS search_to_ad_ratio
            FROM search_demand sd
            LEFT JOIN ad_supply asup ON sd.categoryid = asup."CategoryID"
            ORDER BY sd.total_searches DESC;
        """
    )
    #create_gold_search_vs_supply_table

    #---------C.3. Seasonality - Ads CTR and search frequency by time of day, day of week or month
    # Tracks when demand peaks (searches) and whether ads perform differently across time buckets---------
    
    create_gold_seasonality_table = PostgresOperator(
        task_id="create_gold_seasonality",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_seasonality;
            CREATE TABLE gold_seasonality AS
            SELECT
            DATE_PART('hour', ts.SearchDate::timestamp) AS hour_of_day,
            TO_CHAR(ts.SearchDate::timestamp, 'Day') AS day_of_week,
            DATE_PART('month', ts.SearchDate::timestamp) AS month,
            COUNT(*) AS total_searches,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS total_clicks,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS ctr
            FROM trainsearchstream_silver ts
            GROUP BY hour_of_day, day_of_week, month
            ORDER BY month, day_of_week, hour_of_day;
        """
    )
    #create_gold_seasonality_table
   
    # END OF C. Search and Market Trends---------

    # START OF D. Ad Quality & Pricing Insights---------
    
    #---------D.1. Impact of price on CTR - Average CTR grouped by ad price ranges---------

    create_gold_avgctr_by_price_view = PostgresOperator(
        task_id="create_gold_avgctr_by_price_view",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_avgctr_by_price;
            CREATE VIEW gold_avgctr_by_price AS
            SELECT 
            CASE 
                WHEN s.price < 1000 THEN 'Low'
                WHEN s.price BETWEEN 1000 AND 10000 THEN 'Medium'
                ELSE 'High'
            END AS price_band,
            s.price,
            s.ad_type,
            COUNT(*) AS impressions,
            COUNT(*) FILTER (WHERE s.isclick = 1) AS clicks,
            (COUNT(*) FILTER (WHERE s.isclick = 1))::float / NULLIF(COUNT(*), 0) AS ctr
            FROM trainsearchstream_silver s
            GROUP BY price_band, s.price, s.ad_type
            ORDER BY s.ad_type;
        """
    )
    #create_gold_avgctr_by_price_view
    
    #---------D.2. Effect of Ad type - Compare CTR across free, highlighted, contextual---------

    create_gold_ctr_by_adtype = PostgresOperator(
        task_id="create_gold_ctr_by_adtype",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_ctr_by_adtype;
            CREATE VIEW gold_ctr_by_adtype AS
            SELECT
            ts.ad_type,
            COUNT(ts.*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(ts.*), 0), 2) AS ctr_percent
            FROM trainsearchstream_silver ts
            GROUP BY ts.ad_type
            ORDER BY ctr_percent DESC;
        """
    )
    #create_gold_ctr_by_adtype

    #---------D.3. High CTR but low Conversions - Ads that get clicked often, but don’t lead to visits/phone requests---------
    create_gold_highctr_lowconv_table = PostgresOperator(
        task_id="create_gold_highctr_lowconv",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_highctr_lowconv;
            CREATE TABLE gold_highctr_lowconv AS
            WITH ad_stats AS (
            SELECT
            ts.adid,
            COUNT(*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN vs."UserID" IS NOT NULL OR pr."AdID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions
            FROM trainsearchstream_silver ts
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            GROUP BY ts.adid
            )
        SELECT
            adid,
            impressions,
            clicks,
            conversions,
            ROUND(clicks::numeric * 100.0 / NULLIF(impressions, 0), 2) AS ctr_percent,
            ROUND(conversions::numeric * 100.0 / NULLIF(clicks, 0), 2) AS conversion_rate_percent
        FROM ad_stats
        WHERE clicks > 100  -- filter for meaningful traffic
        AND conversions = 0
        ORDER BY ctr_percent DESC;
        """
    )
    
#---------D.4. Ads with strong engagement but poor conversion - Possible fraud/misleading: many clicks, almost no conversions------
    create_gold_suspicious_ads_table = PostgresOperator(
        task_id="create_gold_suspicios_ads",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_suspicious_ads;
            CREATE TABLE gold_suspicious_ads AS
            WITH ad_metrics AS (
            SELECT
                ts.adid,
                COUNT(*) AS impressions,
                SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
                SUM(CASE WHEN vs."UserID" IS NOT NULL OR pr."AdID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions
            FROM trainsearchstream_silver ts
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            GROUP BY ts.adid
            )
            SELECT
                adid,
                impressions,
                clicks,
                conversions,
                ROUND(clicks::numeric * 100.0 / NULLIF(impressions, 0), 2) AS ctr_percent,
                ROUND(conversions::numeric * 100.0 / NULLIF(clicks, 0), 2) AS conversion_rate_percent,
                CASE 
                    WHEN clicks > 500 AND conversions = 0 THEN 'Potential Fraud'
                    WHEN clicks > 100 AND conversions::numeric / NULLIF(clicks,0) < 0.01 THEN 'Suspiciously Low Conversion'
                    ELSE 'Normal'
                END AS anomaly_flag
            FROM ad_metrics
            ORDER BY ctr_percent DESC;
        """
    )
    #create_gold_highctr_lowconv_table >> create_gold_suspicious_ads_table
 
    # END OF D. Ad Quality & Pricing Insights---------
    
    # START OF E. Location & Category drilldowns---------
    
    #---------E.1. Geo-level performance - CTR, conversion rate, and impressions at city/region/country level------

    create_gold_geo_perf_table = PostgresOperator(
        task_id="create_gold_geo_perf",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_geo_perf;
            CREATE TABLE gold_geo_perf AS
            SELECT 
            ts.regionid AS region,
            ts.cityid AS city,
            COUNT(*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS ctr_percent,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_rate_percent
            FROM trainsearchstream_silver ts
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID" AND ts.adid = vs."AdID"
            GROUP BY ts.regionid, ts.cityid
            ORDER BY impressions DESC;
        """
    )

     #---------E.2. Category hierarchy analysis - Electronics > Mobile Phones > Smartphones → CTR, conversions, demand trend------

    create_gold_category_heirarchy_analysis_table = PostgresOperator(
        task_id="create_gold_category_heirarchy_analysis",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_category_heirarchy_analysis;
            CREATE TABLE gold_category_heirarchy_analysis AS
            SELECT 
            ts.categorylevel,
            ts.categoryid,
            ts.parentcategory,
            ts.subcategory,
            COUNT(*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS ctr_percent,
            SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
            ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_rate_percent,
            DATE_TRUNC('month', ts.SearchDate) AS month,
            COUNT(DISTINCT ts.userid) AS unique_users
            FROM trainsearchstream_silver ts
            LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
            LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID" AND ts.adid = vs."AdID"
            WHERE ts.categorylevel <= 3  -- drill down to Smartphones
            GROUP BY categorylevel, categoryid, parentcategory, subcategory, DATE_TRUNC('month', ts.SearchDate)
            ORDER BY month, impressions DESC;            
        """
    )

    #create_gold_geo_perf_table >> create_gold_category_heirarchy_analysis_table

# END OF E. Location & Category drilldowns---------

# START OF F. Fraud & Anomaly Detection---------
    
#---------F.1.Unusal Activity Detection - Users with abnormally high CTR / clicks in short time for a user such as > 50 impressions in an hour------

    create_gold_unusual_activity_view = PostgresOperator(
        task_id="create_gold_unusual_activity",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_unusual_activity;
            CREATE VIEW gold_unusual_activity AS
            SELECT 
            ts.userid,
            COUNT(*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS ctr_percent,
            MIN(ts.SearchDate) AS first_event,
            MAX(ts.SearchDate) AS last_event,
            EXTRACT(EPOCH FROM (MAX(ts.SearchDate) - MIN(ts.SearchDate))) / 60 AS active_minutes
            FROM trainsearchstream_silver ts
            WHERE ts.userid IS NOT NULL
            GROUP BY ts.userid
            HAVING 
                COUNT(*) > 50 -- at least 50 impressions
                AND (
                    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) > 80
                    OR SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) > 30
                )
            ORDER BY ctr_percent DESC, clicks DESC;
        """
    )

    #---------F.2. Ad Anomaly - Ads with high impressions but 0 clicks (possible poor quality)------

    create_gold_ad_anomaly_view = PostgresOperator(
        task_id="create_gold_ad_anomaly",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_ad_anomaly;
            CREATE VIEW gold_ad_anomaly AS
            SELECT 
            ts.adid,
            ts.title,
            ts.categoryid,
            ts.locationid,
            COUNT(*) AS impressions,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks
            FROM trainsearchstream_silver ts
            GROUP BY ts.adid, ts.title, ts.categoryid, ts.locationid
            HAVING COUNT(*) > 100 AND SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) = 0
        """
    )

    #---------F.3. User session features - Avg. time between search and click for a user------

    create_gold_user_session_anomaly_view = PostgresOperator(
        task_id="create_gold_user_session_anomaly",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_user_session_anomaly;
            CREATE VIEW gold_user_session_anomaly AS
            SELECT
            ts.userid,
            ROUND(AVG(EXTRACT(EPOCH FROM (ts.SearchDate - ts.SearchDate))), 2) AS avg_seconds_to_click
            FROM trainsearchstream_silver ts
            WHERE ts.isclick = 1
            GROUP BY ts.userid
            ORDER BY avg_seconds_to_click;
        """
    )

    #---------F.4. Count of distinct categories searched by a user------

    create_gold_user_difft_categories_anomaly_view = PostgresOperator(
        task_id="create_gold_user_difft_categories_anomaly",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_user_difft_categories_anomaly;
            CREATE VIEW gold_user_difft_categories_anomaly AS
            SELECT
            ts.userid,
            COUNT(DISTINCT ts.categoryid) AS distinct_categories_searched
            FROM trainsearchstream_silver ts
            GROUP BY ts.userid
            ORDER BY distinct_categories_searched DESC;
        """
    )
    # Run these views only once as they are direct views
    #create_gold_unusual_activity_view >> create_gold_ad_anomaly_view >> create_gold_user_session_anomaly_view >> create_gold_user_difft_categories_anomaly_view

# END OF F. Fraud & Anomaly Detection---------

# START OF G. ML Feature Store for Modelling---------
    
#---------G.1.User-level features - Avg CTR, most searched categories, location preference------
    create_gold_user_mlfeatures_table = PostgresOperator(
        task_id="create_gold_user_mlfeatures",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_user_mlfeatures;
            CREATE TABLE gold_user_mlfeatures AS
            -- Step 1: count clicks per category per user
            WITH user_cat_counts AS (
            SELECT
                ts.userid,
                ts.categoryid,
                COUNT(*) AS cnt
            FROM trainsearchstream_silver ts
            GROUP BY ts.userid, ts.categoryid
            ),

        -- Step 2: rank categories per user
            ranked_cats AS (
            SELECT
                userid,
                categoryid,
                cnt,
                ROW_NUMBER() OVER (PARTITION BY userid ORDER BY cnt DESC) AS rn
            FROM user_cat_counts
            )

            -- Step 3: pick top 3 categories per user
            SELECT
                userid,
                ARRAY_AGG(categoryid ORDER BY rn) AS top_categories
            FROM ranked_cats
            WHERE rn <= 3
            GROUP BY userid;
       """
    )

    #---------G.2. Ad-level features - Price, ad type, past CTR, category, location------
    create_gold_ad_mlfeatures_view = PostgresOperator(
        task_id="create_gold_ad_mlfeatures",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_ad_mlfeatures;
            CREATE VIEW gold_ad_mlfeatures AS
            SELECT
            ts.adid,
            ts.price,
            ts.ad_type,
            ts.categoryid,
            ts.locationid,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS ad_past_ctr,
            COUNT(*) AS impressions
            FROM trainsearchstream_silver ts
            GROUP BY ts.adid, ts.price, ts.ad_type, ts.categoryid, ts.locationid;
        """
    )

    #---------G.3. Search-level features - Query text embedding, time of search, category hierarchy------
    create_gold_search_mlfeatures_table = PostgresOperator(
        task_id="create_gold_search_mlfeatures",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP TABLE IF EXISTS gold_search_mlfeatures;
            CREATE TABLE gold_search_mlfeatures AS
            SELECT
            ts.searchid,
            si.searchquery,
            EXTRACT(HOUR FROM ts.SearchDate) AS search_hour,
            EXTRACT(DOW FROM ts.SearchDate) AS day_of_week,
            ts.categorylevel,
            ts.parentcategory,
            ts.subcategory
            FROM trainsearchstream_silver ts
            JOIN SearchInfo si ON ts.searchid = si.searchid
        """
    )

    #---------G.4. Interaction-level features - Position on page, historic CTR at that slot------
    create_gold_interaction_mlfeatures_view = PostgresOperator(
        task_id="create_gold_interaction_mlfeatures",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_interaction_mlfeatures;
            CREATE VIEW gold_interaction_mlfeatures AS
            SELECT
            ts.adid,
            ts.position,
            COUNT(*) AS impressions_at_position,
            SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks_at_position,
            ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS ctr_at_position
            FROM trainsearchstream_silver ts
            GROUP BY ts.adid, ts.position;
        """
    )

    #---------G.5. CTR Prediction ------
    create_gold_ctr_prediction_view = PostgresOperator(
        task_id="create_gold_ctr_prediction",
        postgres_conn_id="my_postgres_conn",
        sql="""
            DROP VIEW IF EXISTS gold_ctr_prediction;
            CREATE VIEW gold_ctr_prediction AS
            SELECT 
            s.id,
            s.searchid,
            s.adid,
            s.userid,
            s.position,
            s.histctr,
            s.isclick,
            s.price,
            s.ad_type,
            s.categoryid,
            s.regionid,
            u."UserAgentID",
            u."UserDeviceID",
            EXTRACT(DOW FROM s.SearchDate) AS day_of_week,
            EXTRACT(HOUR FROM s.SearchDate) AS hour_of_day
        FROM trainsearchstream_silver s
        JOIN "UserInfo" u ON s.userid = u."UserID"
        """
    )

    #creating all tables together under ML feature store
    #create_gold_user_mlfeatures_table >> create_gold_search_mlfeatures_table
    
    #creating all views together under ML feature store - run only once as these are direct views
    #create_gold_ad_mlfeatures_view >> create_gold_interaction_mlfeatures_view >> create_gold_ctr_prediction_view

    # END OF G. ML Feature Store for Modelling---------

    #Tables gold_ctr_ad_perf,gold_top_ads,gold_adtype_revenue for (A) Ad Performance Analytics 
    create_gold_ctr_ad_perf_table
    create_gold_top_ads_table
    create_gold_adtype_revenue_table
    #print("Tables gold_ctr_ad_perf,gold_top_ads,gold_adtype_revenue for (A) Ad Performance Analytics created.")

    #Tables gold_user_engagement_metrics,gold_conversion_funnel for (B) User Behavior Insights
    create_gold_user_engagement_metrics_table
    create_gold_conversion_funnel_table
    #print("Tables gold_user_engagement_metrics,gold_conversion_funnel for (B) User Behavior Insights created.")

    #Tables gold_search_trends,gold_search_vs_supply,gold_seasonality for (C) Search and Market Trends
    create_gold_search_trends_table
    create_gold_search_vs_supply_table
    create_gold_seasonality_table
    #print("Tables gold_search_trends,gold_search_vs_supply,gold_seasonality for (C) Search and Market Trends created.")

    #Tables gold_highctr_lowconv,gold_suspicious_ads for (D) Ad Quality & Pricing Insights
    create_gold_highctr_lowconv_table
    create_gold_suspicious_ads_table
    #print("Tables gold_highctr_lowconv,gold_suspicious_ads for (D) Ad Quality & Pricing Insights created.")

    #Tables gold_geo_perf,gold_category_heirarchy_analysis for (E) Location & Category Drilldowns
    create_gold_geo_perf_table
    create_gold_category_heirarchy_analysis_table
    #print("Tables gold_geo_perf,gold_category_heirarchy_analysis for (E) Location & Category Drilldowns created.")

    #Tables gold_user_mlfeatures,gold_search_mlfeatures for (G) ML Feature Store
    create_gold_user_mlfeatures_table
    create_gold_search_mlfeatures_table
    #print("Tables gold_user_mlfeatures,gold_search_mlfeatures for (G) ML Feature Store created.")

 