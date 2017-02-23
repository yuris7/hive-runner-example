CREATE EXTERNAL TABLE IF NOT EXISTS agg_details_visual_daily(
user_type string,
age tinyint,
gender char(1),
contenttype string,
platform string,
category string,
genre string,
region string,
state string,
total_details_views int,
total_start_video int
    )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_details_visual_daily';

--------------------------------------------------------------------------------

INSERT INTO TABLE  agg_details_visual_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
        user_type,
        floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	    gender,
       lcts_data.contenttype,
       lcts_data.platform,
       category,
       genre,
       --COALESCE(category, channel_category)  AS category,
       --COALESCE(genre, channel_genre) AS genre,
       --channel_name AS channel,
       regionname AS region,
       state,
       COUNT (lcts_data.lcts_et) AS total_details_views,
       SUM(CASE WHEN lcts_data.nonlcts_et IS NULL THEN 0 ELSE 1 END) AS total_start_video
FROM
    (SELECT lcts.userid,
            lcts.user_type,
            lcts.sessionid,
            lcts.appversion,
            lcts.contentid,
            lcts.contenttype,
            lcts.platform,
            lcts.eventtype AS lcts_et,
            non_lcts.eventtype AS nonlcts_et,
            lcts.min_timestamp AS lcts_min_ts
     FROM
         (SELECT userid,
                 IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
                 sessionid,
                 appversion,
                 contentid,
                 contenttype,
                 eventtype,
                 platform,
                 min(TIMESTAMP) AS min_timestamp
          FROM user_action
          WHERE partition_date= '${hiveconf:ENDDATE}'
              AND eventtype = 'LOADCONTENTDETAILSPAGE'
              AND (contenttype NOT LIKE 'LIVE%' AND contenttype <> 'LINEAR')
          GROUP BY userid,
                   customerid,
                   sessionid,
                   appversion,
                   contentid,
                   contenttype,
                   eventtype,
                   platform) lcts
     FULL OUTER JOIN
         (SELECT userid,
                 IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
                 sessionid,
                 appversion,
                 contentid,
                 contenttype,
                 eventtype,
                 platform,
                 max(TIMESTAMP) AS max_timestamp
          FROM user_action
          WHERE partition_date= '${hiveconf:ENDDATE}'
              AND eventtype = 'VIDEOSTART'
              AND (contenttype NOT LIKE 'LIVE%' AND contenttype <> 'LINEAR')
          GROUP BY userid,
                   customerid,
                   sessionid,
                   appversion,
                   contentid,
                   contenttype,
                   eventtype,
                   platform) non_lcts ON lcts.userid = non_lcts.userid
     AND lcts.appversion = non_lcts.appversion
     AND lcts.contentid = non_lcts.contentid
     AND lcts.platform = non_lcts.platform
     AND lcts.sessionid = non_lcts.sessionid
     WHERE lcts.min_timestamp IS NOT NULL
    AND (non_lcts.max_timestamp IS NULL
    OR lcts.min_timestamp < non_lcts.max_timestamp)
 )lcts_data
LEFT OUTER JOIN
    (SELECT categoryname AS category,
            genre,
            contentid
     FROM vod_catalog)CONTENT ON CONTENT.contentid = lcts_data.contentid
--LEFT OUTER JOIN tvchannels ON  tvchannels.channel_id =  lcts_data.contentid
LEFT OUTER JOIN profiling ON profiling.userid = lcts_data.userid
GROUP BY 
         lcts_data.user_type,
         floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
         gender,
         lcts_data.contenttype,
         lcts_data.platform,
         category,
         genre,
         --COALESCE(category, channel_category),
         --COALESCE(genre, channel_genre),
         --channel_name,
         regionname,
         state;

--------------------------------------------------------------------------------


CREATE EXTERNAL TABLE IF NOT EXISTS agg_details_visual_weekly(
user_type string,
age tinyint,
gender char(1),
contenttype string,
platform string,
category string,
genre string,
region string,
state string,
total_details_views int,
total_start_video int,
partition_date string
    )
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_details_visual_weekly';

--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE agg_details_visual_weekly PARTITION (YEAR = '${hiveconf:YEAR}', week)
SELECT user_type,
        age,
        gender,
       contenttype,
       platform,
       category,
       genre,
       region,
       STATE,
       SUM(total_details_views),
       SUM(total_start_video),
       '${hiveconf:ENDDATE}' AS partition_date,
       weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) AS week
FROM agg_details_visual_daily
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
    AND '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type,
        age,
        gender,
         contenttype,
         platform,
         category,
         genre,
         region,
         state;

--------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS agg_details_visual_monthly(
user_type string,
age tinyint,
gender char(1),
contenttype string,
platform string,
category string,
genre string,
region string,
state string,
total_details_views int,
total_start_video int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_details_visual_monthly';

--------------------------------------------------------------------------------

INSERT OVERWRITE TABLE agg_details_visual_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT user_type,
        age,
        gender,
       contenttype,
       platform,
       category,
       genre,
       region,
       state,
       SUM(total_details_views),
       SUM(total_start_video),
       '${hiveconf:ENDDATE}' AS partition_date
FROM agg_details_visual_daily
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type,
        age,
        gender,
         contenttype,
         platform,
         category,
         genre,
         region,
         state;

--------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS agg_details_visual_timeband(
user_type string,
age tinyint,
gender char(1),
contenttype string,
platform string,
category string,
genre string,
region string,
state string,
timeband string,
total_details_views int,
total_start_video int
    )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_details_visual_timeband';

--------------------------------------------------------------------------------

INSERT INTO TABLE  agg_details_visual_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
        lcts_data.user_type,
        floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
        gender,
       lcts_data.contenttype,
       lcts_data.platform,
       category,
       genre,
       regionname AS region,
       state,
       lcts_data.tb AS timeband,
       COUNT (lcts_data.lcts_et) AS total_details_views,
       SUM(CASE WHEN lcts_data.nonlcts_et IS NULL THEN 0 ELSE 1 END) AS total_start_video
FROM
    (SELECT lcts.userid,
            lcts.user_type,
            lcts.sessionid,
            lcts.appversion,
            lcts.contentid,
            lcts.contenttype,
            lcts.platform,
            lcts.eventtype AS lcts_et,
            non_lcts.eventtype AS nonlcts_et,
            lcts.min_timestamp AS lcts_min_ts,
            (CASE WHEN substr(lcts.min_timestamp,9,2) BETWEEN 0 AND 3 THEN '0to4'
            WHEN substr(lcts.min_timestamp,9,2) BETWEEN 4 AND 7 THEN '4to8'
            WHEN substr(lcts.min_timestamp,9,2) BETWEEN 8 AND 11 THEN'8to12'
            WHEN substr(lcts.min_timestamp,9,2) BETWEEN 12 AND 15 THEN '12to16'
            WHEN substr(lcts.min_timestamp,9,2) BETWEEN 16 AND 19 THEN'16to20' ELSE '20to24' END) AS tb
     FROM
         (SELECT userid,
                 IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
                 sessionid,
                 appversion,
                 contentid,
                 contenttype,
                 eventtype,
                 platform,
                 min(TIMESTAMP) AS min_timestamp
          FROM user_action
          WHERE partition_date= '${hiveconf:ENDDATE}'
              AND eventtype = 'LOADCONTENTDETAILSPAGE'
              AND (contenttype NOT LIKE 'LIVE%' AND contenttype <> 'LINEAR')
          GROUP BY userid,
                   customerid,
                   sessionid,
                   appversion,
                   contentid,
                   contenttype,
                   eventtype,
                   platform) lcts
     FULL OUTER JOIN
         (SELECT userid,
                 IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
                 sessionid,
                 appversion,
                 contentid,
                 contenttype,
                 eventtype,
                 platform,
                 max(TIMESTAMP) AS max_timestamp
          FROM user_action
          WHERE partition_date= '${hiveconf:ENDDATE}'
              AND eventtype = 'VIDEOSTART'
              AND (contenttype NOT LIKE 'LIVE%' AND contenttype <> 'LINEAR')
          GROUP BY userid,
                   customerid,
                   sessionid,
                   appversion,
                   contentid,
                   contenttype,
                   eventtype,
                   platform) non_lcts 
    ON lcts.userid = non_lcts.userid
    AND lcts.appversion = non_lcts.appversion
    AND lcts.contentid = non_lcts.contentid
    AND lcts.platform = non_lcts.platform
    AND lcts.sessionid = non_lcts.sessionid
    WHERE lcts.min_timestamp IS NOT NULL
    AND (non_lcts.max_timestamp IS NULL 
    OR lcts.min_timestamp < non_lcts.max_timestamp)
 )lcts_data
LEFT OUTER JOIN
    (SELECT categoryname AS category,
            genre,
            contentid
     FROM vod_catalog)CONTENT ON CONTENT.contentid = lcts_data.contentid
LEFT OUTER JOIN profiling ON profiling.userid = lcts_data.userid
GROUP BY lcts_data.user_type,
        floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
        gender,
         lcts_data.contenttype,
         lcts_data.platform,
         category,
         genre,
         regionname,
         state,
         lcts_data.tb;
