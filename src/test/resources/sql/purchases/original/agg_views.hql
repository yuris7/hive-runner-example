---
-- table structure. Timeband job
---

CREATE EXTERNAL TABLE IF NOT EXISTS agg_views_timeband(
user_type string,
age tinyint,
gender char(1),
platform string,
category string,
genre string,
contentype string,
state string,
region string,
appversion string,
channel string,
views int,
playback_duration decimal(15,4),
playback_per_view decimal(15,4),
downloads int,
trailer_views int,
timeband string
    )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_timeband';

----
-- timeband aggregated 
----

INSERT INTO TABLE  agg_views_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
 SELECT
 	user_type,
 	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
 	platform,
 	COALESCE(categoryname, channel_category) AS category, 
	COALESCE(genre, channel_genre) AS genre,
 	contentype,
 	state,
 	regionname as region,
  	appversion,
  	channel_name as channel,
 	COUNT(CASE WHEN download != 'Y' THEN watching.userid END) AS views, --#A_74 #A_75 #A_108 #A_116 | A_73,A_76, A_77, A_78, A_79, A_84 dimensions exclude downloads
 	sum(CASE WHEN download != 'Y' THEN consumption END)/60 as playback_duration,  --#A_99 | A_101, A_102, A_103, A_104  dimensions --convert seconds to minutes
 	(sum(CASE WHEN download != 'Y' THEN consumption END)/60)/ COUNT(CASE WHEN download != 'Y' THEN watching.userid END) AS playback_per_view, -- #A_100
 	SUM(IF(download = 'Y',1,0)) AS  downloads,
 	COUNT(CASE WHEN contentype = 'TRAILER' THEN watching.userid END) as trailer_views,
 	--COUNT( DISTINCT watching.userid)/COUNT(watching.userid) AS user_RATE
 	timeband
 FROM(
 		SELECT
 			userid,
 			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
 			contentid,
 			contentype,
 			platform,
 			(CASE WHEN substr(timestamp,9,2) between 0 and 3 THEN '0to4' 
			WHEN substr(timestamp,9,2) between 4 and 7 THEN '4to8'
			WHEN substr(timestamp,9,2) between 8 and 11 THEN'8to12'
			WHEN substr(timestamp,9,2) between 12 and 15 THEN '12to16'
			WHEN substr(timestamp,9,2) between 16 and 19 THEN'16to20'
			ELSE '20to24' END)  AS  timeband,   --Time slots of a day.
     			download,
         		consumption,
          		appversion
 		FROM watching
 		WHERE partition_date= '${hiveconf:ENDDATE}'  -- date of aggregations specified
 		)watching
 	LEFT OUTER JOIN vod_catalog ON watching.contentid = vod_catalog.contentid
 	LEFT OUTER JOIN profiling LOC ON LOC.userid = watching.userid --merge tables on key = userid
 	LEFT OUTER JOIN tvchannels on  tvchannels.channel_id =  watching.contentid
 GROUP BY   -- group data by dimensions
 	user_type,
 	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
 	gender,
 	COALESCE(categoryname, channel_category), 
	COALESCE(genre, channel_genre),
 	state,
 	regionname,
  	appversion,
 	platform,
 	contentype,
 	timeband, 
 	channel_name;

-----
--Structure for daily KPIs
-----

CREATE EXTERNAL TABLE IF NOT EXISTS agg_views_daily( 
user_type string,
age tinyint,
gender char(1),
platform string,
category string,
genre string,
contentype string,
state string,
region string,
appversion string,
channel string,
views int,
playback_duration decimal(15,4),
playback_per_view decimal(15,4),
downloads int,
trailer_views int
    )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_daily';

------
--Daily Aggregates
------
INSERT INTO TABLE  agg_views_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT  
	user_type,
	age,
 	gender,
	platform, 
	COALESCE(categoryname, channel_category) AS category, 
	COALESCE(genre, channel_genre) AS genre,
	contentype,
	state, 
	region,
	appversion,
	channel_name as channel,
	--
	COUNT(CASE WHEN download != 'Y' THEN watching.userid END) AS views, --#A_74 #A_75 #A_108 #A_116 | A_5,A_6,A_8,A_9,A_73,A_76, A_77, A_78, A_79, A_84 +dimensions exclude downloads
--	COUNT(DISTINCT CASE WHEN download != 'Y' THEN watching.userid END) AS unique_users, --#A_105 exclude download events
	sum(CASE WHEN download != 'Y' THEN consumption END)/60 as playback_duration,  --#A_99 | A_101, A_102, A_103, A_104 + dimensions --convert seconds to minutes
	(sum(CASE WHEN download != 'Y' THEN consumption END)/60)/ COUNT(CASE WHEN download != 'Y' THEN watching.userid END) AS playback_per_view, -- #A_100
	SUM(IF(download = 'Y',1,0)) AS  downloads,
	COUNT(CASE WHEN contentype = 'TRAILER' THEN watching.userid END) as trailer_views
	--COUNT( DISTINCT watching.userid)/COUNT(watching.userid) AS user_RATE
FROM(
		SELECT 
			userid,
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
			contentid, 
			contentype,
			platform,
			timestamp,
			download,
			consumption,
			appversion
		FROM watching
		WHERE partition_date= '${hiveconf:ENDDATE}'  -- date of aggregations specified
		)watching
	LEFT OUTER JOIN vod_catalog ON watching.contentid = vod_catalog.contentid 
	LEFT OUTER JOIN tvchannels on  tvchannels.channel_id =  watching.contentid
	LEFT OUTER JOIN( --select location related dim. 
	SELECT 
		userid, 
		floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 		gender,
		state, 
		regionname as region 
	FROM profiling 
	)LOC
ON LOC.userid = watching.userid --merge tables on key = userid
GROUP BY   -- group data by dimensions
	user_type, 
	age,
	gender,
	COALESCE(categoryname, channel_category), 
	COALESCE(genre, channel_genre),
	state,
	region,
	platform, 
	contentype,
	appversion,
	channel_name;


---
--Structure for  weekly KPIs
---
CREATE TABLE IF NOT EXISTS agg_views_weekly( 
user_type string,
age tinyint,
gender char(1),
platform string,
category string,
genre string,
contentype string,
state string,
region string,
appversion string,
channel string,
views int,
playback_duration decimal(15,4), 
playback_per_view decimal(15,4),
downloads int,
trailer_views int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_weekly';



----
--Weekly aggregates
--based on Daily agg.
---

INSERT OVERWRITE  TABLE  agg_views_weekly PARTITION  (year = '${hiveconf:YEAR}', week)
SELECT  
	user_type,
	age,
	gender,
	platform,	
	category,	
	genre,
	contentype,	
	state,	
	region,
	appversion,
	channel,
	SUM(views) as  views,
	SUM(playback_duration) as  playback_duration,
	SUM(playback_duration)/SUM(views)  as playback_per_view,
	SUM(downloads) as downloads,
	SUM(trailer_views) as trailer_views,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM agg_views_daily --usage of daily aggr. kpis with the aim to reduce processing time
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type, age,gender,platform,category,genre,contentype,state,region,appversion, channel;

----
--table structure for mothly aggr.
---
CREATE TABLE IF NOT EXISTS agg_views_monthly( 
user_type string,
age tinyint,
gender char(1),
platform string,
category string,
genre string,
contentype string,
state string,
region string,
appversion string,
channel string,
views int,
playback_duration decimal(15,4), 
playback_per_view decimal(15,4),
--user_rate float,
downloads int,
trailer_views int,
partition_date string
)
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_monthly';

----
--Monthly aggregations
----

INSERT OVERWRITE  TABLE  agg_views_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT 
	user_type,
	age,
	gender,
	platform,	
	category,	
	genre,
	contentype,	
	state,	
	region,
	appversion,
	channel,
	SUM(views) AS  views,
	SUM(playback_duration) AS  playback_duration,
	SUM(playback_duration)/SUM(views)  AS playback_per_view,
	SUM(downloads) AS downloads,
	SUM(trailer_views) AS trailer_views,
	'${hiveconf:ENDDATE}' as partition_date
FROM agg_views_daily --based on daily preaggregated data
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type, age,gender,platform,category,genre,contentype,state,region,appversion, channel; --grouped by dimensions

---
-- structure for daily A_105,A_108
---
CREATE EXTERNAL TABLE IF NOT EXISTS agg_views_user_button_daily( 
user_type string,
age tinyint,
gender char(1),
contentype string,
state string, 
region string,  
views int, 
unique_users int
    )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_user_button_daily';

---
--daily aggregtion for  agg_view user button  #A_108  & #A_105
---

INSERT INTO TABLE  agg_views_user_button_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT  
	IF(WATCHING.customerid = 'GUEST', 'GUEST', 'REGISTERED') as user_type,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	gender,
	contentype,
	state, 
	regionname as region,  
	COUNT(WATCHING.userid) AS views, --exclude downloads #A_108
	COUNT(DISTINCT  WATCHING.userid) AS unique_users --#A_105 exclude download events
	FROM WATCHING --initial data set is split in two - for registered and anonymous
LEFT OUTER JOIN profiling LOC
ON LOC.userid = WATCHING.userid --merge tables on key = userid
WHERE partition_date= '${hiveconf:ENDDATE}' and  download != 'Y'-- date of aggregations specified
GROUP BY  
	IF(WATCHING.customerid = 'GUEST', 'GUEST', 'REGISTERED'),
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state, 
	regionname, 
	contentype;

---
--structure for weekly aggregation
---

CREATE TABLE IF NOT EXISTS agg_views_user_button_weekly( 
user_type string,
age tinyint,
gender char(1),
contentype string,
state string, 
region string,  
views int, 
unique_users int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_user_button_weekly';

---
--weekly aggr.
---

INSERT OVERWRITE TABLE  agg_views_user_button_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT  
	IF(watching.customerid = 'GUEST', 'GUEST', 'REGISTERED') as user_type, --user type can be set to Registered/Guest - done automatically based on table data selected from 
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	gender,
	contentype,
	state, 
	regionname as region,  
	COUNT(watching.userid) AS views, --exclude downloads #A_108
	COUNT(DISTINCT  watching.userid) AS unique_users, --#A_105 exclude download events
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM  watching --initial data set is split in two - for registered and anonymous
LEFT OUTER JOIN profiling LOC
ON LOC.userid = watching.userid --merge tables on key = userid
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
AND download != 'Y' 
GROUP BY  
	IF(watching.customerid = 'GUEST', 'GUEST', 'REGISTERED'),
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state, 
	regionname,
	contentype;

---
--structure for weekly aggregation
---

CREATE TABLE IF NOT EXISTS agg_views_user_button_monthly( 
user_type string,
age tinyint,
gender char(1),
contentype string,
state string, 
region string,  
views int, 
unique_users int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_views_user_button_monthly';

---
--weekly aggr.
---

INSERT OVERWRITE TABLE  agg_views_user_button_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT  
	IF(watching.customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type, --user type can be set to Registered/Guest - done automatically based on table data selected from 
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	gender,
	contentype,
	state, 
	regionname as region,  
	COUNT(watching.userid) AS views, --exclude downloads #A_108
	COUNT(DISTINCT  watching.userid) AS unique_users, --#A_105 exclude download events
	'${hiveconf:ENDDATE}' as partition_date
FROM  watching
LEFT OUTER JOIN profiling LOC
ON LOC.userid = watching.userid --merge tables on key = userid
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
AND download != 'Y'
GROUP BY  
	IF(watching.customerid = 'GUEST', 'GUEST', 'REGISTERED'),
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state,
	regionname,
	contentype;

---
--structure for timeband related aggr.
---

CREATE EXTERNAL TABLE IF NOT EXISTS views_after_trailer_timeband(
user_type string,
age tinyint,
gender char(1),
appversion string,
contentype string,
platform string,
category string,
genre string,
state string,
region string,
timeband string,    
views_after_trailer int
    )
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/views_after_trailer_timeband';

---
-- timeband aggr.  A_285 A_270
---

INSERT INTO TABLE views_after_trailer_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
select 
	user_type,
	age,
	gender,
	appversion,
	contentype,
	platform,
	category,
	genre,
	state,
	region,
	timeband,
	count(views_after_trailer.userid) as  views_after_trailer
FROM (
	SELECT 
		trailer_view.userid,
		trailer_view.user_type,
		trailer_view.appversion,
		trailer_view.contentid,
		trailer_view.platform,
		non_trailer.contentype,
		CASE WHEN trailer_view.hour between 0 and 3 THEN '0to4' 
		WHEN trailer_view.hour between 4 and 7 THEN '4to8'
		WHEN trailer_view.hour between 8 and 11 THEN'8to12'
		WHEN trailer_view.hour between 12 and 15 THEN '12to16'
		WHEN trailer_view.hour between 16 and 19 THEN'16to20'
		ELSE '20to24' END  AS  timeband
	FROM (
		SELECT 
			userid,
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
			sessionid,
			appversion,
			contentid, 
			contentype,
			timestamp,
			substr(timestamp,9,2) as hour,
			platform
		FROM  WATCHING
		WHERE partition_date= '${hiveconf:ENDDATE}'
		AND contentype = 'TRAILER'
		)trailer_view
	LEFT OUTER JOIN(
		SELECT 
			userid,
			sessionid,
			appversion,
			contentid, 
			contentype,
			timestamp,
			substr(timestamp,9,2) as hour,
			platform
		FROM WATCHING
		WHERE partition_date= '${hiveconf:ENDDATE}'
		AND contentype != 'TRAILER'
		AND download != 'Y'
		)non_trailer
	ON	trailer_view.userid = non_trailer.userid
	AND trailer_view.appversion = non_trailer.appversion
	AND trailer_view.contentid = non_trailer.contentid
	AND trailer_view.platform = non_trailer.platform
	AND trailer_view.sessionid = non_trailer.sessionid
	--AND trailer_view.hour= non_trailer.hour
	WHERE trailer_view.timestamp < non_trailer.timestamp
GROUP BY 
	trailer_view.user_type,
	trailer_view.userid,
	trailer_view.appversion,
	trailer_view.platform,
	trailer_view.contentid, 
	trailer_view.hour,
	non_trailer.contentype 
	)views_after_trailer
LEFT OUTER JOIN ( --select required dim for contentid
	SELECT
		categoryname AS category,
		genre,
		contentid
	FROM  vod_catalog
    )CONTENT
ON views_after_trailer.contentid = CONTENT.contentid --merge data sets on key  = contentid
LEFT OUTER JOIN( --select location related dim. 
	SELECT 
		userid, 
		floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
		gender,
		state, 
		regionname as region
	FROM profiling 
	)LOC
ON LOC.userid = views_after_trailer.userid 
group by 
	user_type,
	age,
	gender,
	appversion, 
	contentype, 
	platform,
	category,genre,
	state, 
	region, 
	timeband;

---
--table structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS views_after_trailer_daily(
user_type string,
age tinyint,
gender char(1),
appversion string,
contentype string,
platform string,
category string,
genre string,
state string,
region string,
views_after_trailer int)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/views_after_trailer_daily';

---
-- #A_285, A_270 daily.
---

INSERT INTO TABLE views_after_trailer_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	user_type,
	age,
	gender,
	appversion, 
	contentype, 
	platform,
	category,
	genre,
	state, 
	region,
	COUNT(views_after_trailer.userid) AS  views_after_trailer
FROM (
	SELECT 
		trailer_view.userid,
		trailer_view.user_type,
		trailer_view.appversion,
		trailer_view.contentid,
		trailer_view.platform,
		non_trailer.contentype
	FROM (
		SELECT 
			userid,
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
			sessionid,
			appversion,
			contentid, 
			contentype,
			timestamp,
			platform
		FROM  watching --aggr. based on watching subsets - registered/anonymous related
		WHERE partition_date= '${hiveconf:ENDDATE}' --specify date
		AND contentype = 'TRAILER' --select all TRAILER related entries
		)trailer_view
		LEFT OUTER JOIN(
		SELECT 
			userid,
			sessionid,
			appversion,
			contentid, 
			contentype,
			timestamp,
			platform
		FROM  watching
		WHERE partition_date= '${hiveconf:ENDDATE}'
		AND contentype != 'TRAILER' --select all  TRAILER non related entries
		AND download != 'Y' --exclude download events
		)non_trailer
	ON trailer_view.userid = non_trailer.userid --merge TRAILER/ non-TRAILER entries on  key = dimensions
	AND trailer_view.appversion = non_trailer.appversion
	AND trailer_view.contentid = non_trailer.contentid
	AND trailer_view.platform = non_trailer.platform
	AND trailer_view.sessionid = non_trailer.sessionid
	WHERE trailer_view.timestamp < non_trailer.timestamp --select only those entries where TRAILER view appeared before content view
	GROUP BY 
	trailer_view.userid,
	trailer_view.user_type,
	trailer_view.appversion,
	trailer_view.platform,
	trailer_view.contentid, 
	non_trailer.contentype 
	)views_after_trailer
LEFT OUTER JOIN ( --select required dim for contentid
SELECT
	categoryname AS category,
	genre,
	contentid
FROM   vod_catalog
)CONTENT
ON views_after_trailer.contentid = CONTENT.contentid --merge data on key  = contentid
LEFT OUTER JOIN( --select location related dim. 
SELECT 
	userid, 
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	gender,
	state, 
	regionname as region
FROM profiling 
)LOC
ON LOC.userid = views_after_trailer.userid 
GROUP BY 
	user_type,
	age,
	gender,
	appversion, 
	contentype, 
	platform,
	category,
	genre,
	state, 
	region;

---
-- structure for weekly aggr. A_285, A_270
---

CREATE TABLE IF NOT EXISTS views_after_trailer_weekly(
user_type string,
age tinyint,
gender char(1),
appversion string,
contentype string,
platform string,
category string,
genre string,
state string,
region string,
views_after_trailer int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/views_after_trailer_weekly';

---
-- weekly aggregates
---

INSERT OVERWRITE TABLE views_after_trailer_weekly PARTITION  (year = '${hiveconf:YEAR}', week)
SELECT
	user_type,
	age,
	gender,
	appversion,
	contentype,
	platform,
	category,
	genre,
	state,
	region,
	sum(views_after_trailer) as views_after_trailer,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM views_after_trailer_daily --aggregations based on daily pre - aggregates to reduce processing time 
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
	user_type, 
	age, gender,
	appversion, contentype,
	platform, category, genre,
	state, region;

---
-- structure for monthly aggr. A_285, A_270
---
	
CREATE TABLE IF NOT EXISTS views_after_trailer_monthly(
user_type string,
age tinyint,
gender char(1),
appversion string,
contentype string,
platform string,
category string,
genre string,
state string,
region string,
views_after_trailer int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/views_after_trailer_monthly';

---
-- monthly aggregates
---

INSERT OVERWRITE TABLE views_after_trailer_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT
	user_type,
	age,
	gender,
	appversion,
	contentype,
	platform,
	category,
	genre,
	state,
	region,
	sum(views_after_trailer) as views_after_trailer,
	'${hiveconf:ENDDATE}' as partition_date
FROM views_after_trailer_daily --aggregations based on daily pre - aggregates to reduce processing time 
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
	user_type, 
	age,
	gender,
	appversion, 
	contentype,
	platform, 
	category, 
	genre,
	state, 
	region;
