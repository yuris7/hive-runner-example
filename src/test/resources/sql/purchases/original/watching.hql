-- creation of target table for watching entity
CREATE EXTERNAL TABLE IF NOT EXISTS watching (
  tenantid STRING,
  customerid STRING,
  userid STRING,
  username STRING,
  smartcardid STRING,
  timestamp STRING,
  platform STRING,
  externalcontentid STRING,
  consumption STRING,
  parametersection STRING,
  sessionid STRING,
  contentype STRING,
  solutionofferid STRING,
  providername STRING,
  servicename STRING,
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  appversion STRING,
  contentid STRING,
  customercode STRING,
  devicemake STRING,
  deviceyear STRING,
  download STRING,
  isfree STRING
)
--- PARTITIONED BY (partition_date STRING)
--- ROW FORMAT
--- DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--- LINES TERMINATED BY '\n'
--- STORED AS TEXTFILE
--- LOCATION '${hiveconf:ROOTPATH}/AVA/WATCHING/input'
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/original/WATCHING_20160302.CSV' OVERWRITE INTO TABLE watching PARTITION (partition_date='20080815');

-- creation of rejected table for watching entity
CREATE EXTERNAL TABLE IF NOT EXISTS watching_rejected (
  tenantid STRING,
  customerid STRING,
  userid STRING,
  username STRING,
  smartcardid STRING,
  timestamp STRING,
  platform STRING,
  externalcontentid STRING,
  consumption STRING,
  parametersection STRING,
  sessionid STRING,
  contentype STRING,
  solutionofferid STRING,
  providername STRING,
  servicename STRING,
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  appversion STRING,
  contentid STRING,
  customercode STRING,
  devicemake STRING,
  deviceyear STRING,
  download STRING,
  isfree STRING,  
  rejected_reason STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/WATCHING/rejected'


-- Add new partitions
-- msck repair table watching
-- msck repair table watching_rejected

