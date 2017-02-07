-- creation of target table for vod_catalog entity
CREATE EXTERNAL TABLE IF NOT EXISTS vod_catalog (
  contentid STRING,
  title STRING,
  categoryname STRING,
  genre STRING,
  contentduration DOUBLE
)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE
