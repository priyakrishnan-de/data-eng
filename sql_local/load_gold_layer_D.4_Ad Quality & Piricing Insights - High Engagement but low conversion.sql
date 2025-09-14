
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--D.4 --Ad Quality & Pricing Insights - Strong engagement but poor conversion---

--Ads with strong engagement but poor conversion--
--Possible fraud/misleading: many clicks, almost no conversions--


WITH ad_metrics AS (
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
    ROUND(conversions::numeric * 100.0 / NULLIF(clicks, 0), 2) AS conversion_rate_percent,
    CASE 
        WHEN clicks > 500 AND conversions = 0 THEN 'Potential Fraud'
        WHEN clicks > 100 AND conversions::numeric / NULLIF(clicks,0) < 0.01 THEN 'Suspiciously Low Conversion'
        ELSE 'Normal'
    END AS anomaly_flag
FROM ad_metrics
ORDER BY ctr_percent DESC;


