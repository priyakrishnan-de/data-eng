
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--D.2 --Ad Quality & Pricing Insights - Effect of Ad type on CTR---

--Effect of Ad type – Compare CTR across Free, Highlighted, Contextual

SELECT
    ts.ad_type,
    COUNT(ts.*) AS impressions,
    SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(ts.*), 0), 2) AS ctr_percent
FROM trainsearchstream_silver ts
GROUP BY ts.ad_type
ORDER BY ctr_percent DESC;


