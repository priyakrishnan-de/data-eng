
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--F.1 --Fraud and Anomaly - Ads with lots of impressions but 0 clicks---

--Ads with lots of impressions but 0 clicks

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
