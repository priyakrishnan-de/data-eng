--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--C.1 --Search and Market Trends - Categories and Locations and Time---

--Search Trends – Category * Location * Time
CREATE TABLE gold_search_trends AS
SELECT 
    s.SearchDate::date AS day,
    s.categoryid,
    s.regionid,
    COUNT(DISTINCT s.searchid) AS total_searches,
    COUNT(*) FILTER (WHERE s.isclick = 1) AS total_clicks,
    (COUNT(*) FILTER (WHERE s.isclick = 1))::float / NULLIF(COUNT(*), 0) AS ctr
FROM trainsearchstream_silver s
GROUP BY s.SearchDate::date, s.categoryid, s.regionid;
