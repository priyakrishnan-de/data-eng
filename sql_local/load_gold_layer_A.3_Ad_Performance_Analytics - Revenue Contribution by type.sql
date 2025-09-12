
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--A.2 ---Ad performance – Revneue Contribution by type
--Revnue Contribution by type

--Assumption: consider a “conversion” generates revenue equal to price

SELECT
    ts.ad_type,
    COUNT(ts.adid) AS impressions,
    SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN ts.price ELSE 0 END) AS revenue,
    SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN ts.price ELSE 0 END)::numeric / NULLIF(SUM(ts.price),0) AS revenue_share
FROM trainsearchstream_silver ts
LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
GROUP BY ts.ad_type
ORDER BY revenue DESC;
