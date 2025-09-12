
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--A.1 ---Ad performance – Daily CTR and Conversions by adtype----

--Provides click count, impressions count, ctr, distinct Visits count, distinct phone requests – for Ad, Ad type, params, price, title, viewDate

create INDEX idx_searchinfo_searchid ON searchInfo(searchid); // 2 min 25 sec
create INDEX idx_phone_adid ON "PhoneRequestsStream"("AdID"); // 16.2 sec
create INDEX idx_phone_userid ON "PhoneRequestsStream"("UserID"); //11.2 sec

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
    ON c.adid = pr."AdID"
   AND c.userid = pr."UserID"
   AND c.day = pr.day
ORDER BY c.day, c.adid, c.userid;

--Query // 8.2 sec

