-- creation of target table for user_action entity
CREATE EXTERNAL TABLE IF NOT EXISTS user_action (
  tenantid STRING,
  customerid STRING,
  userid STRING,
  username STRING,
  timestamp STRING,
  sessionid STRING,
  devicetype STRING,
  deviceid STRING,
  devicemodel STRING,
  devicemake STRING,
  deviceos_firmwareversion STRING,
  deviceipaddress STRING,
  appversion STRING,
  platform STRING,
  eventtype STRING,
  eventduration DOUBLE,
  licensetype STRING,
  licensereqlatency DOUBLE,
  contenttype STRING,
  contentsubtype STRING,
  contentid STRING,
  contenturl STRING,
  contenttitle STRING,
  videosessionid STRING,
  playerconnectiontime DOUBLE,
  appconnectiontime DOUBLE,
  seekrequestposition DOUBLE,
  seekcompleteposition DOUBLE,
  playbackstopposition DOUBLE,
  terminationreason STRING,
  contentrentallatency DOUBLE,
  searchstring STRING,
  searchlatency DOUBLE,
  epglatency DOUBLE,
  npvrlistlatency DOUBLE,
  npvraction STRING,
  airingid STRING,
  airingstarttime STRING,
  airingduration DOUBLE,
  npvractionlatency DOUBLE,
  downloadstatus STRING,
  downloadreason STRING,
  downloadduration DOUBLE,
  adurl STRING,
  hpsectionarea STRING
)
PARTITIONED BY (partition_date STRING)
ROW FORMAT
DELIMITED FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/tmp/AVA/USERACTION/input'