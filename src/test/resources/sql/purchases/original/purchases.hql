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
--- PARTITIONED BY (partition_date STRING)
--- ROW FORMAT
--- DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--- LINES TERMINATED BY '\n'
--- STORED AS TEXTFILE
--- LOCATION '${hiveconf:ROOTPATH}/AVA/PURCHASE/input';
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/PURCHASE_20160302.csv' OVERWRITE INTO TABLE purchase PARTITION (partition_date='20080815');


-- creation of rejected table for purchase entity
CREATE EXTERNAL TABLE IF NOT EXISTS purchase_rejected (
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
  commerce_model STRING,
  rejected_reason STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/PURCHASE/rejected'



--- msck repair table purchase
--- msck repair table purchase_rejected