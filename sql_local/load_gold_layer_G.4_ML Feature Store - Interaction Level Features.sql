--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--G.4 --ML Feature Store (For training ML models) - Interaction Level Features---

--Interaction Level Features--

--Position on page, historic CTR at that slot--

SELECT
    ts.adid,
    ts.position,
    COUNT(*) AS impressions_at_position,
    SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks_at_position,
    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0), 4) AS ctr_at_position
FROM trainsearchstream_silver ts
GROUP BY ts.adid, ts.position;

