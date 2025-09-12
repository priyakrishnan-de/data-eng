
--THIS QUERY IS USED IN AIRFLOW DAG load-delta_silver.py-----------


--Old query for loading to silver
--963 rows – takes 45 secs
--After indexing, takes 0.87 sec

SELECT stg.id, stg.searchid, stg.adid, stg.position, stg.objecttype, stg.histctr, stg.isclick,
        a.params, a.title, a.price,a."LocationID", a."CategoryID",
        si.ipid, si.userid, si.searchquery, si.searchparams
	    FROM trainsearchstream_staging stg
	    LEFT JOIN AdsInfo a ON stg.adid = a.adid
        LEFT JOIN SearchInfo si on (stg.searchid = si.searchid AND a."LocationID" = si."LocationID" 
		and a."CategoryID" = si."CategoryID")

--Indexes done:
CREATE INDEX idx_stg_adid ON trainsearchstream_staging(adid);
CREATE INDEX idx_stg_searchid ON trainsearchstream_staging(searchid);
CREATE INDEX idx_ads_adid ON AdsInfo(adid); //35+ sec
-- composite index to help with join filter
CREATE INDEX idx_ads_loc_cat ON AdsInfo("LocationID", "CategoryID"); //44 sec
CREATE INDEX idx_si_searchid ON SearchInfo(searchid); // 1 min 26 sec
-- composite index to help with join filter
CREATE INDEX idx_si_loc_cat ON SearchInfo("LocationID", "CategoryID"); // 1 min 50 sec

--New query for loading to silver

SELECT stg.id, stg.searchid, stg.adid, stg.position, stg.objecttype, stg.histctr, stg.isclick,
        a.params, a.title, a.price,a."LocationID", a."CategoryID",
        si.ipid, si.userid, si.searchquery, si.searchparams,si."SearchDate",
		c."Level" as CategoryLevel, c."ParentCategoryID" as ParentCategory, c."SubcategoryID" as SubCategory, l."Level" as Locationlevel, l."RegionID", l."CityID"
        FROM trainsearchstream_staging stg
	    LEFT JOIN AdsInfo a ON stg.adid = a.adid
        LEFT JOIN SearchInfo si on stg.searchid = si.searchid 
		LEFT JOIN "Category" c ON a."CategoryID" = c."CategoryID"
		LEFT JOIN "Location" l ON a."LocationID" = l."LocationID"
