-- creation of target table for purchase entity
CREATE EXTERNAL TABLE IF NOT EXISTS purchase (
  tenantid STRING,
  customerid STRING,
  userid STRING,
  username STRING,
  purchasetime STRING,
  sessionid STRING,
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  devicemake STRING,
  deviceos_firmwareversion STRING,
  deviceipaddress STRING,
  appversion STRING,
  platform STRING,
  contentid STRING,
  paymenttype STRING,
  transactionid STRING,
  discountedprice DECIMAL(15,2),
  originalprice DECIMAL(15,2),
  currency STRING,
  state STRING,
  contenttype STRING,
  providername STRING,
  servicename STRING,
  solutionofferid STRING,
  commerce_model STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/PURCHASE_20160302.csv' OVERWRITE INTO TABLE purchase PARTITION (partition_date='20080815');

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