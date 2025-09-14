
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--F.3 --Fraud and Anomaly - User Session features---

--User Session feature 1 - Avg. time between search and click--

SELECT
    ts.userid,
    ROUND(AVG(EXTRACT(EPOCH FROM (ts.SearchDate - ts.SearchDate))), 2) AS avg_seconds_to_click
FROM trainsearchstream_silver ts
WHERE ts.isclick = 1
GROUP BY ts.userid
ORDER BY avg_seconds_to_click;


-- User Session Feature 2 - Number of distinct categories searched per user--

SELECT
    ts.userid,
    COUNT(DISTINCT ts.categoryid) AS distinct_categories_searched
FROM trainsearchstream_silver ts
GROUP BY ts.userid
ORDER BY distinct_categories_searched DESC;
