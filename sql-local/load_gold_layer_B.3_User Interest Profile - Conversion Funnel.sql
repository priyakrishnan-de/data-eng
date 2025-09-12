
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--B.3 --User Interest Profile – Conversion Funnel---

--Conversion Funnel - Search → Impression → Click → Visit → Phone Request

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
