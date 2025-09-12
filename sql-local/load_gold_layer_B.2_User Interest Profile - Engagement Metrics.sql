
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--B.2 --User Interest Profile – Engagement Metrics---

--Engagement metrics - Avg. impressions per search, avg. CTR per user, repeat visits

create index idx_user_id ON trainsearchstream_silver(userid);
create index idx_visits_user_id ON "VisitsStream"("UserID") --index took 3 min 41 sec

WITH user_stats AS (
    SELECT
        ts.userid,
        COUNT(ts.adid) AS impressions,
        SUM(CASE WHEN ts.isclick = 1 THEN 1 ELSE 0 END) AS clicks,
        COUNT(DISTINCT ts.searchid) AS searches,
        COUNT(DISTINCT vs."UserID" || '-' || vs."AdID" || '-' || vs."ViewDate") AS visits
    FROM trainsearchstream_silver ts
    LEFT JOIN "VisitsStream" vs ON ts.userid = vs."UserID"
    GROUP BY ts.userid
)
SELECT
    AVG(impressions::numeric / NULLIF(searches,0)) AS avg_impressions_per_search,
    AVG(clicks::numeric / NULLIF(impressions,0)) AS avg_ctr_per_user,
    AVG(visits) AS avg_repeat_visits_per_user
FROM user_stats;

--Even after indexing ~ 56.7 sec
