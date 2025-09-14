
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--F.1 --Fraud and Anomaly - Unusual Activity Detection---

--Detect users with unusually high CTR (e.g., > 80%) or too many clicks in a short time window (say 1 hour) - 
--Flags potential bots, click farms, or fraudulent users--

--Users with abnormally high CTR or click bursts--

SELECT 
    ts.userid,
    COUNT(*) AS impressions,
    SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS ctr_percent,
    MIN(ts.SearchDate) AS first_event,
    MAX(ts.SearchDate) AS last_event,
    EXTRACT(EPOCH FROM (MAX(ts.SearchDate) - MIN(ts.SearchDate))) / 3600 AS active_minutes
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

