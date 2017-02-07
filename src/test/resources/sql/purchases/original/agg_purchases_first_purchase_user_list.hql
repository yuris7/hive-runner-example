---
--Create a list of users with first purchase date
---

CREATE EXTERNAL TABLE IF NOT EXISTS first_purchase_user_list( 
userid string,
first_purchase_date string
)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/first_purchase_user_list';


INSERT INTO TABLE first_purchase_user_list  PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT DISTINCT prch.userid
	,prch.partition_date
FROM purchase prch
WHERE prch.partition_date = '${hiveconf:ENDDATE}'
	AND

NOT EXISTS (
		SELECT userid
		FROM first_purchase_user_list fpul
		WHERE prch.userid = fpul.userid
		);
