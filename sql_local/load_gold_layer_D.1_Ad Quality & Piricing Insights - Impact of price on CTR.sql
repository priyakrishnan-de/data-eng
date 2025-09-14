
--THIS QUERY IS USED IN AIRFLOW DAG load_gold_layer.py-----

--D.1 --Ad Quality & Pricing Insights - Impact of Price on CTR---

--Ad Quality Vs Price – Impact of Price on CTR

SELECT 
    CASE 
        WHEN s.price < 1000 THEN 'Low'
        WHEN s.price BETWEEN 1000 AND 10000 THEN 'Medium'
        ELSE 'High'
    END AS price_band,
	s.price,
    s.ad_type,
    COUNT(*) AS impressions,
    COUNT(*) FILTER (WHERE s.isclick = 1) AS clicks,
    (COUNT(*) FILTER (WHERE s.isclick = 1))::float / NULLIF(COUNT(*), 0) AS ctr
FROM trainsearchstream_silver s
GROUP BY price_band, s.price, s.ad_type
ORDER BY s.ad_type;
