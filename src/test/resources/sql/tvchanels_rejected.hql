-- creation of target table for tvchannels entity (flume dir)
CREATE EXTERNAL TABLE IF NOT EXISTS tvchannels_rejected (
 channel_id STRING,
 channel_name STRING,
 channel_type STRING,
 channel_category STRING,
 channel_genre STRING,
 channel_number STRING,
 channel_network STRING,
 rejected_reason STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${hiveconf:ROOTPATH}/AVA/TVCHANNELS/rejected'
