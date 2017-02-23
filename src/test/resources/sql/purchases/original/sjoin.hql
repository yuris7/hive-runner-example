-- creation of target table for join entity  -- TABLE JOIN WAS CHANGED TO  SJOIN IN sjoin.hql & agg_registrations.hql FILES
CREATE EXTERNAL TABLE IF NOT EXISTS sjoin (
  tenantid STRING,
  date STRING,
  time STRING,
  loginemail STRING,
  clientipaddress STRING,
  username STRING,
  uniquecontract STRING,
  timestamp STRING,
  servicename STRING,
  providername STRING,
  referralsource STRING,
  registrationmethod STRING,
  registrationtrigger STRING
)
--- PARTITIONED BY (partition_date STRING)
--- ROW FORMAT DELIMITED
--- FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--- LINES TERMINATED BY '\n'
--- STORED AS TEXTFILE
--- LOCATION '${hiveconf:ROOTPATH}/AVA/JOIN/input'
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/original/JOIN_20160302.CSV' OVERWRITE INTO TABLE sjoin PARTITION (partition_date='20080815');

-- creation of rejected table for join entity
CREATE EXTERNAL TABLE IF NOT EXISTS join_rejected (
  tenantid STRING,
  date STRING,
  time STRING,
  loginemail STRING,
  clientipaddress STRING,
  username STRING,
  uniquecontract STRING,
  timestamp STRING,
  servicename STRING,
  providername STRING,
  referralsource STRING,
  registrationmethod STRING,
  registrationtrigger STRING,
  rejected_reason STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/JOIN/rejected'




-- Add new partitions
--- msck repair table sjoin -- Commented - it coflicts with custom partition
--- msck repair table join_rejected