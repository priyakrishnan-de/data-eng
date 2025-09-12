
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--B.1 --User Interest Profile – Category & Location preferences---

CREATE VIEW  gold_user_interest_profile AS
SELECT 
    u."UserID",
	COUNT(DISTINCT s.searchid) AS total_searches,
    COUNT(*) FILTER (WHERE s.isclick = 1) AS total_clicks,
    ARRAY_AGG(DISTINCT s.categoryid) AS preferred_categories,
    ARRAY_AGG(DISTINCT s.regionid) AS preferred_locations,
    AVG(CASE WHEN s.isclick = 1 THEN 1 ELSE 0 END)::float AS avg_ctr
FROM trainsearchstream_silver s
JOIN "UserInfo" u ON s.userid = u."UserID"
GROUP BY u."UserID";
