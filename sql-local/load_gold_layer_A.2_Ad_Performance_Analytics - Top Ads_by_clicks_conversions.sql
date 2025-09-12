
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--A.2 ---Ad performance – Top Ads by clicks and conversions

--Top Ads by clicks / conversions

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
