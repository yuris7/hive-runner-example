-- creation of target table for vod_catalog entity
CREATE EXTERNAL TABLE IF NOT EXISTS vod_catalog (
  contentid STRING,
  title STRING,
  categoryname STRING,
  genre STRING,
  contentduration DOUBLE
)
-- ROW FORMAT
-- DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
-- LINES TERMINATED BY '\n' 
-- STORED AS TEXTFILE

PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/VOD_CATALOG_20160302.CSV' OVERWRITE INTO TABLE vod_catalog PARTITION (partition_date='20080815');


--- 'src/test/resources/sql/purchases/VOD_CATALOG_20160302.CSV'