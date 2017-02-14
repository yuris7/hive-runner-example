-- creation of target table for profiling entity
CREATE TABLE IF NOT EXISTS profiling (
  timestamp_server STRING,
  customerid STRING,
  username STRING,
  userid STRING,
  master_user STRING,
  first_name STRING,
  surname STRING,
  gender STRING,
  mobile_phone STRING,
  email_address STRING,
  birth_date STRING,
  parental_control_status STRING,
  vod_parental_control_set STRING,
  epg_parental_control_set STRING,
  payment_method STRING,
  personalized_services_flag STRING,
  marketing_flag STRING,
  third_parties_flag STRING,
  first_line_address STRING,
  regionid INT,
  regionname STRING,
  country STRING,
  state STRING,
  province STRING,
  city STRING,
  zip_code STRING,
  preferred_language STRING
)
-- ROW FORMAT DELIMITED
-- FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
-- LINES TERMINATED BY '\n'
-- STORED AS TEXTFILE
-- LOCATION '${hiveconf:ROOTPATH}/AVA/PROFILING_TARGET/input';

PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/PROFILING_20160302.CSV' OVERWRITE INTO TABLE profiling PARTITION (partition_date='20080815');
