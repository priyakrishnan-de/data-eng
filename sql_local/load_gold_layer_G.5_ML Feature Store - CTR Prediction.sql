
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--G.5 --ML Feature Store (For training ML models) - CTR Prediction---

--CTR Prediction--

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


