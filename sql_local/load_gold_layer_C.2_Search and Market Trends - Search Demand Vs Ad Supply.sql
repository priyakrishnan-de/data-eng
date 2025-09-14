
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--C.2 --Search and Market Trends - Search Demand Vs Ad Supply---

--search_to_ad_ratio highlights whether demand is outpacing supply (high ratio = undersupply)

WITH search_demand AS (
    SELECT
        ts.categoryid,
        COUNT(*) AS total_searches
    FROM trainsearchstream_silver ts
    GROUP BY ts.categoryid
),
ad_supply AS (
    SELECT
        ads."CategoryID",
        COUNT(*) AS total_ads
    FROM AdsInfo ads
    GROUP BY ads."CategoryID"
)
SELECT
    sd.categoryid,
    sd.total_searches,
    COALESCE(asup.total_ads, 0) AS total_ads,
    ROUND(sd.total_searches::numeric / NULLIF(asup.total_ads, 0), 2) AS search_to_ad_ratio
FROM search_demand sd
LEFT JOIN ad_supply asup ON sd.categoryid = asup."CategoryID"
ORDER BY sd.total_searches DESC;


