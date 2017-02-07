--=============================
--data preprocessing for weekly/monthly/yearly aggr
--goal - reduce processing time - instead of raw data aggr. to detect unique users
--this table - containing list of unique users and latest purchase day - can be used:
--=============================
---
--structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS purchase_user_list(
userid string,
customerid string,
currency string,
latest_purchase_date string
)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/purchase_user_list';

---
--list of unique userid|customerid + day of latest purchase
---
--table contains information  about currency each user use, date when lates purchase was made - created to reduce complexity of further aggregations
--for ex. to detect amount of purchases by unique users without processing 90 days of raw data
INSERT OVERWRITE TABLE purchase_user_list
SELECT
	userid,
	customerid,
	currency,
	MAX(latest_purchase_date) AS latest_purchase_date
FROM(
	SELECT
		userid,
		customerid,
		currency,
		date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0) AS latest_purchase_date
	FROM  purchase
	WHERE partition_date = '${hiveconf:ENDDATE}'
UNION  ALL
    SELECT * FROM purchase_user_list existed_purch
    )new_purchases
GROUP BY userid, customerid, currency;