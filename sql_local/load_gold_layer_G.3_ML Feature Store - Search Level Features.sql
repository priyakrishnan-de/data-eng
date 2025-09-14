
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--G.3 --ML Feature Store (For training ML models) - Search Level Features---

--Search Level features--

--Query embedding, time of search, category hierarchy--

SELECT
    ts.searchid,
    si.searchquery,
    EXTRACT(HOUR FROM ts.SearchDate) AS search_hour,
    EXTRACT(DOW FROM ts.SearchDate) AS day_of_week,
    ts.categorylevel,
    ts.parentcategory,
    ts.subcategory
FROM trainsearchstream_silver ts
JOIN SearchInfo si ON ts.searchid = si.searchid
