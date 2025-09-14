
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--E.1 --Location & Category Drilldowns - Geo Level Performance---

--Geo level performance
--CTR, conversion rate, impressions aggregated by city, region, country.

-- This select query took 48 sec before indexing and 0.751 sec after indexing--


CREATE INDEX idx_pr_user_ad ON "PhoneRequestsStream"( "UserID", "AdID" ); --14 sec
CREATE INDEX idx_vs_user ON "VisitsStream"( "UserID","AdID" ); --5 min 50 sec
CREATE INDEX idx_tss_region_city ON trainsearchstream_silver(regionid, cityid); --0.051 s

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

