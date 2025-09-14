
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--D.3 --Ad Quality & Pricing Insights - High CTR but Low Conversion---

--High CTR but low Conversion
--Ads that get clicked often, but don’t lead to visits/phone requests

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

