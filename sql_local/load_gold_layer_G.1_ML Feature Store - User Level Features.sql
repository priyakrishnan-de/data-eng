
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--G.1 --ML Feature Store (For training ML models) - User Level Features---

--User Level features--
--Average CTR, most searched categories, location preference per user--

-- Step 1: count clicks per category per user
WITH user_cat_counts AS (
    SELECT
        ts.userid,
		ts.categoryid,
        COUNT(*) AS cnt
    FROM trainsearchstream_silver ts
    GROUP BY ts.userid, ts.categoryid
),

-- Step 2: rank categories per user
ranked_cats AS (
    SELECT
        userid,
        categoryid,
        cnt,
        ROW_NUMBER() OVER (PARTITION BY userid ORDER BY cnt DESC) AS rn
    FROM user_cat_counts
)

-- Step 3: pick top 3 categories per user
SELECT
    userid,
    ARRAY_AGG(categoryid ORDER BY rn) AS top_categories
FROM ranked_cats
WHERE rn <= 3
GROUP BY userid;

