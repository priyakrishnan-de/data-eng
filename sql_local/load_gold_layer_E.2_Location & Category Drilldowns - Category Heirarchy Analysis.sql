
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--E.2 --Location & Category Drilldowns - Category Heirarchy Analysis---

--CTR, conversions, trend across the hierarchy (Electronics → Mobile Phones → Smartphones)--

--Assumptions--
--Level = 1 → Top-level category (e.g., Electronics)--
--Level = 2 → Sub-category (e.g., Mobile Phones)--
--Level = 3 → Leaf category (e.g., Smartphones)--

SELECT 
    ts.categorylevel,
    ts.categoryid,
    ts.parentcategory,
    ts.subcategory,
    COUNT(*) AS impressions,
    SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
    ROUND(SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS ctr_percent,
    SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END) AS conversions,
    ROUND(SUM(CASE WHEN pr."AdID" IS NOT NULL OR vs."UserID" IS NOT NULL THEN 1 ELSE 0 END)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) AS conversion_rate_percent,
    DATE_TRUNC('month', ts.SearchDate) AS month,
    COUNT(DISTINCT ts.userid) AS unique_users
FROM trainsearchstream_silver ts
LEFT JOIN "PhoneRequestsStream" pr ON ts.userid = pr."UserID" AND ts.adid = pr."AdID"
LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID" AND ts.adid = vs."AdID"
WHERE ts.categorylevel <= 3  -- drill down to Smartphones
GROUP BY categorylevel, categoryid, parentcategory, subcategory, DATE_TRUNC('month', ts.SearchDate)
ORDER BY month, impressions DESC;

