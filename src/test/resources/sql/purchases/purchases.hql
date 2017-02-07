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
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/PURCHASE/input';