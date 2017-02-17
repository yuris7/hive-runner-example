-- creation of target table for join entity
CREATE EXTERNAL TABLE IF NOT EXISTS `join` (
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
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/JOIN/input';

-- creation of rejected table for join entity
CREATE EXTERNAL TABLE IF NOT EXISTS `join_rejected` (
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
LOCATION '${hiveconf:ROOTPATH}/AVA/JOIN/rejected';

-- Add new partitions
msck repair table `join`;
msck repair table `join_rejected`;