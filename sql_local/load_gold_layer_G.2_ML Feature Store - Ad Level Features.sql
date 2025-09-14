
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--G.2 --ML Feature Store (For training ML models) - Ad Level Features---

--Ad Level features--

Price, ad type, past CTR, category, location
SELECT
    ts.adid,
    ts.price,
    ts.ad_type,
    ts.categoryid,
    ts.locationid,
    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS ad_past_ctr,
    COUNT(*) AS impressions
FROM trainsearchstream_silver ts
GROUP BY ts.adid, ts.price, ts.ad_type, ts.categoryid, ts.locationid;


