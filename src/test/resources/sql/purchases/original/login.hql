-- creation of target table for login entity
CREATE EXTERNAL TABLE IF NOT EXISTS login (
  tenantid STRING,
  userid STRING,
  username STRING,
  smartcardid STRING,
  timestamp STRING,
  platform STRING,
  sessionid STRING,
  providername STRING,
  servicename STRING, 
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  devicemake STRING,
  deviceosfirmwareversion STRING,
  deviceipaddress STRING,
  appversion STRING,
  deviceyear STRING,
  loginsuccess STRING,
  loginfailure STRING,
  eventtype STRING,
  transmissiontype STRING
)
PARTITIONED BY (partition_date STRING)
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
-- LINES TERMINATED BY '\n'
-- STORED AS TEXTFILE
-- LOCATION '${hiveconf:ROOTPATH}/AVA/LOGIN/input'

ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/original/LOGIN_20160302.CSV' OVERWRITE INTO TABLE login PARTITION (partition_date='20080815');

-- creation of rejected table for login entity
CREATE EXTERNAL TABLE IF NOT EXISTS login_rejected (
  tenantid STRING,
  userid STRING,
  username STRING,
  smartcardid STRING,
  timestamp STRING,
  platform STRING,
  sessionid STRING,
  providername STRING,
  servicename STRING, 
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  devicemake STRING,
  deviceosfirmwareversion STRING,
  deviceipaddress STRING,
  appversion STRING,
  deviceyear STRING,
  loginsuccess STRING,
  loginfailure STRING,
  eventtype STRING,
  transmissiontype STRING,
  rejected_reason STRING
)
PARTITIONED BY (partition_date STRING)
--- ROW FORMAT DELIMITED
--- FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--- LINES TERMINATED BY '\n'
--- STORED AS TEXTFILE
--- LOCATION '${hiveconf:ROOTPATH}/AVA/LOGIN/rejected'




-- Add new partitions
-- msck repair table login
-- msck repair table login_rejected
