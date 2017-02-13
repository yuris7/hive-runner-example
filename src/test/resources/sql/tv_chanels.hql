-- creation of target table for tvchannels entity
CREATE EXTERNAL TABLE IF NOT EXISTS tvchannels (
 channel_id STRING,
 channel_name STRING,
 channel_type STRING,
 channel_category STRING,
 channel_genre STRING,
 channel_number STRING,
 channel_network STRING
)
--ROW FORMAT
--DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--LINES TERMINATED BY '\n'
--STORED AS TEXTFILE
--LOCATION '${hiveconf:ROOTPATH}/AVA/TVCHANNELS_TARGET/input';

--ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
--LOAD DATA LOCAL INPATH 'sql/TVCHANNELS_20160302.csv' OVERWRITE INTO TABLE tvchannels;

PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/original/TVCHANNELS_20160302.csv' OVERWRITE INTO TABLE tvchannels PARTITION (partition_date='20080815');