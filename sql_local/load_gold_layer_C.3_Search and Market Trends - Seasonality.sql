
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--C.3 --Search and Market Trends - Seasonality---

--Seasonality – Search frequency & CTR by hour, weekday, month--

--Tracks when demand peaks (searches) and whether ads perform differently across time buckets

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

