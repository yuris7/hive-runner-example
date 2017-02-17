---
-- table timeband structure
---

CREATE EXTERNAL TABLE IF NOT EXISTS agg_logins_timeband( 
state string,
region string,
platform string,
age tinyint,
gender char(1),
appversion string,
logged_accesses int,
successful_logins int,
failed_logins int,
timeband string
)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_logins_timeband';

---
-- timeband aggegations
--A_28,A_29, A_30, A_31, A_50
--

INSERT INTO TABLE agg_logins_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	state, 
	regionname as region,
	platform,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
	appversion,
	--amount of LOGGED, FAILLED, ALL accesses for each set of dim.
	SUM(accesses) AS logged_accesses,
	SUM(successful_logins) AS successful_logins,
	SUM(failed_logins) AS failed_logins,
	timeband
FROM(
	SELECT
		userid,
		platform,
		appversion,
		COUNT(userid) AS accesses,
		COUNT(CASE WHEN LOGINSUCCESS='Y' THEN 1 END) AS successful_logins, --count amount of all LOGINSUCCESS='Y' for each user
		COUNT(CASE WHEN LOGINSUCCESS='N' THEN 1 END) AS failed_logins,
		(CASE WHEN substr(timestamp,9,2) between 0 and 3 THEN '0to4' 
		WHEN substr(timestamp,9,2) between 4 and 7 THEN '4to8'
		WHEN substr(timestamp,9,2) between 8 and 11 THEN'8to12'
		WHEN substr(timestamp,9,2) between 12 and 15 THEN '12to16'
		WHEN substr(timestamp,9,2) between 16 and 19 THEN'16to20'
		ELSE '20to24' END)  AS  timeband   --Time slots of a day.			
	FROM login
	WHERE partition_date = '${hiveconf:ENDDATE}' --specify target day
	AND eventtype  = 'LOGIN'
	GROUP BY userid, appversion, platform, substr(timestamp,9,2)
	)user_logins
LEFT OUTER JOIN
profiling prof -- join location data on key = userid
ON  prof.userid = user_logins.userid
GROUP BY 
platform,
floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
gender,
appversion,
timeband, 
state, 
regionname;

---
-- structure for daily KPIs
----

CREATE EXTERNAL TABLE IF NOT EXISTS agg_logins_daily( 
state string,
region string,
platform string,
age tinyint,
gender char(1),
appversion string,
logged_accesses int,
successful_logins int,
failed_logins int
)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_logins_daily';

-----
-- DAILY A_28, A_29, A_50, A_30, A_31, A_21
-----

INSERT INTO TABLE agg_logins_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	state, 
	regionname as region,
	platform,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
	appversion,
	--amount of LOGGED, FAILLED, ALL accesses for each set of location related  dim.
	SUM(accesses) AS logged_accesses ,
	SUM(successful_logins) AS successful_logins,
	SUM(failed_logins) AS failed_logins
FROM(
	SELECT
		userid,
		platform,
		appversion,
		COUNT(userid) AS accesses,
		COUNT(CASE WHEN LOGINSUCCESS='Y' THEN 1 END) AS successful_logins, --count amount of all LOGINSUCCESS='Y' for each user
		COUNT(CASE WHEN LOGINSUCCESS='N' THEN 1 END) AS failed_logins    
	FROM login
	WHERE partition_date ='${hiveconf:ENDDATE}' --specify target day
	AND eventtype  = 'LOGIN'
	GROUP BY userid, appversion, platform	
	)user_logins
LEFT OUTER JOIN
profiling prof -- join location data on key = userid
ON  prof.userid = user_logins.userid
GROUP BY 
state, 
floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
gender,
regionname,
appversion, 
platform;

----
--table structure for weekly KPIs
----
CREATE TABLE IF NOT EXISTS agg_logins_weekly( 
state string,
region string,
platform string,
age tinyint,
gender char(1),
appversion string,  
logged_accesses  int,
successful_logins int,
failed_logins int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_logins_weekly';

-----
-- WEEKLY  A_28, A_29, A_50, A_30, A_31, A_21
-----
INSERT OVERWRITE TABLE agg_logins_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT
	state,
	region,
	platform,
	age,
	gender,
	appversion, 
	sum(logged_accesses) as logged_accesses,
	sum(successful_logins) as successful_logins,
	sum(failed_logins) as failed_logins,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM agg_logins_daily --aggregations based on daily pre aggregates.
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
platform,
age, 
gender,
appversion,
state,
region;

----
--table structure for weekly KPIs
----
CREATE TABLE IF NOT EXISTS agg_logins_monthly( 
state string,
region string,
platform string,
age tinyint,
gender char(1),
appversion string,  
logged_accesses  int,
successful_logins int,
failed_logins int,
partition_date string
)
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_logins_monthly';

-----
-- MONTHLY  A_28, A_29, A_50, A_30, A_31, A_21
-----
INSERT OVERWRITE TABLE agg_logins_monthly PARTITION (month = '${hiveconf:YYYYMM}')
SELECT
	state,
	region,
	platform,
	age,
	gender,
	appversion, 
	sum(logged_accesses) as logged_accesses,
	sum(successful_logins) as successful_logins,
	sum(failed_logins) as failed_logins,
	'${hiveconf:ENDDATE}' as partition_date
FROM agg_logins_daily --aggregations based on daily pre aggregates.
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
platform,
age,
gender,
appversion,
state,
region;

--
--table structure daily A_42,A_43
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_multiple_platform_daily( 
state string,
region string,
logged_users_more_platforms int,
distinct_users int
    )
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_multiple_platform_daily';

--
--Daily A_42,A_43
---
INSERT INTO TABLE accesses_multiple_platform_daily PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT
	state,
	regionname AS region,
	COUNT(CASE WHEN cnt_platform >1 THEN 1 END) AS logged_users_more_platforms,
	COUNT(DISTINCT multi_platforms.userid ) AS distinct_users --amount of users who use several platforms for each location
FROM (
	SELECT 
		userid,
		COUNT(DISTINCT platform) AS cnt_platform --amount of different platforms per user
	FROM login
	WHERE partition_date= '${hiveconf:ENDDATE}' --specify target day
	AND eventtype  = 'LOGIN'
	GROUP BY userid
	)multi_platforms
LEFT OUTER JOIN profiling  --add location data for users who use more platforms
ON profiling.userid = multi_platforms.userid 
GROUP BY  
	regionname, 
	state;
	

---
--table structure
---
CREATE TABLE IF NOT EXISTS accesses_multiple_platform_weekly( 
state string,
region string,
logged_users_more_platforms int,
distinct_users int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_multiple_platform_weekly';

--
--Weekly A_42,A_43
---
INSERT OVERWRITE TABLE accesses_multiple_platform_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT
	state,
	regionname AS region,
	COUNT(CASE WHEN cnt_platform >1 THEN 1 END) AS logged_users_more_platforms,
	COUNT(DISTINCT multi_platforms.userid ) AS distinct_users, --amount of users who use several platforms for each location
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM (
	SELECT 
		userid,
		COUNT(DISTINCT platform) AS cnt_platform --amount of different platforms per user
	FROM login
	WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
	AND eventtype  = 'LOGIN'
	GROUP BY userid
	)multi_platforms
LEFT OUTER JOIN profiling  --add location data for users who use more platforms
ON profiling.userid = multi_platforms.userid 
GROUP BY  
	regionname, 
	state; 

---
--table structure
---
CREATE TABLE IF NOT EXISTS accesses_multiple_platform_monthly( 
state string,
region string,
logged_users_more_platforms int,
distinct_users int,
partition_date string
)
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_multiple_platform_monthly';

--
--Monthly A_42,A_43
---
INSERT OVERWRITE TABLE accesses_multiple_platform_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT
	state,
	regionname AS region,
	COUNT(CASE WHEN cnt_platform >1 THEN 1 END) AS logged_users_more_platforms,
	COUNT(DISTINCT multi_platforms.userid ) AS distinct_users, --amount of users who use several platforms for each location
	'${hiveconf:ENDDATE}' as partition_date
FROM (
	SELECT 
		userid,
		COUNT(DISTINCT platform) AS cnt_platform --amount of different platforms per user
	FROM login
	WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
	AND eventtype  = 'LOGIN'
	GROUP BY userid
	)multi_platforms
LEFT OUTER JOIN profiling  --add location data for users who use more platforms
ON profiling.userid = multi_platforms.userid 
GROUP BY  
	regionname, 
	state; 

CREATE TABLE  IF NOT EXISTS login_devices(
userid string,
deviceid string,
first_access_day string,
last_access_day string )
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/login_devices';


INSERT OVERWRITE TABLE login_devices
SELECT
	userid,
	deviceid,
	min(first_access_day) as first_access_day,
	max(last_access_day) as last_access_day
FROM(
	SELECT 
		userid,
		deviceid,
		first_access_day,
		last_access_day
	FROM login_devices 
	WHERE first_access_day <> '${hiveconf:ENDDATE}' 
UNION ALL 
	SELECT
		userid,
		deviceid,
		partition_date as first_access_day,
		partition_date as last_access_day
	FROM login
	WHERE partition_date = '${hiveconf:ENDDATE}' 
	AND eventtype  = 'LOGIN'
	AND loginsuccess = 'Y'
	)access_day_update
GROUP BY 
	userid,
	deviceid;
------
-- SCHEMA FOR ANONYMOUS USERS/ACTIVITY 
-------
CREATE TABLE  IF NOT EXISTS anonymous_devices(
userid string,
deviceid string,
first_access_day string,
last_access_day string )
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/anonymous_devices';

-------
--LIST OF UNIQUE ANONYMOUS DEVICES, FIRST/LAST DAY OF ACTIVITY
-------
	
INSERT OVERWRITE TABLE anonymous_devices
SELECT
	userid,
	deviceid,
	min(first_access_day) as first_access_day,
	max(last_access_day) as last_access_day
FROM(
	SELECT 
		userid,
		deviceid,
		first_access_day,
		last_access_day
	FROM anonymous_devices
	WHERE first_access_day <> '${hiveconf:ENDDATE}' 
UNION ALL 
	SELECT
		userid,
		deviceid,
		partition_date as first_access_day,
		partition_date as last_access_day
	FROM user_action
	WHERE partition_date = '${hiveconf:ENDDATE}' 
	AND eventtype  = 'LOADHOME'
	AND terminationreason = '' 
  AND customerid = 'GUEST' 
	)access_day_update
GROUP BY 
	userid,
	deviceid;
	
-----------

CREATE EXTERNAL TABLE IF NOT EXISTS agg_accesses_daily(
user_type string,
age tinyint,
gender char(1),
state string,
region string,
platform string,
accesses bigint,
accesses_with_views bigint,
accesses_without_views bigint)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_daily';

INSERT INTO TABLE agg_accesses_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	if(gender is null, 'GUEST', 'REGISTERED') AS user_type,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) AS age,
	gender,
	state,
	regionname AS region,
	platform,
	sum(unique_sessions) AS accesses,
	sum(accesses_with_views) AS accesses_with_views,
	sum(accesses_without_views) AS accesses_without_views
FROM(
    SELECT  
		if(login_devices.deviceid is null, loadhome.userid, login_devices.userid) AS userid,
		loadhome.deviceid AS deviceid,
		count(distinct loadhome.sessionid) AS unique_sessions,
		platform,
		-- if(loadhome.userid = login_devices.userid, 'N', 'Y')  AS access_as_anonymous,
		sum(if(videostart.deviceid is null, 0,1)) AS accesses_with_views,
		sum(if(videostart.deviceid is null, 1,0)) AS accesses_without_views
		FROM(
		SELECT          
			userid, 
			deviceid,
			sessionid,
			customerid,
			platform
		FROM user_action 
		WHERE partition_date = '${hiveconf:ENDDATE}' -- specify target day
		AND eventtype = 'LOADHOME'  -- select "access platform" entries
		AND terminationreason = '' -- delect successful accesses
		GROUP BY 
			userid, 
			deviceid,
			sessionid,
			customerid,
			platform
			)loadhome
		LEFT OUTER JOIN( 
		SELECT 
			deviceid,
			collect_list(userid)[0]  AS userid
		FROM  login_devices
		GROUP BY deviceid
		)  login_devices
		ON  login_devices.deviceid = loadhome.deviceid	
		LEFT OUTER JOIN (
		SELECT
			sessionid,
			deviceid
		FROM user_action 
		WHERE partition_date = '${hiveconf:ENDDATE}'
		AND eventtype = 'VIDEOSTART' --select entries related to content view
		AND contentsubtype != 'TRAILER'
		GROUP BY 
			sessionid,
			deviceid
		)videostart
		ON  loadhome.deviceid = videostart.deviceid -- merge 'LOADHOME' with 'VIDEOSTART' on sessionid and deviceid
		AND loadhome.sessionid =  videostart.sessionid 
  GROUP BY 
  loadhome.deviceid, loadhome.userid,
  login_devices.userid, login_devices.deviceid, 
  platform
	)viewership
LEFT OUTER JOIN profiling  ON profiling.userid = viewership.userid
    GROUP BY 
	gender,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state,
	regionname,
	platform;
	
-------
--SCHEMA FOR TIMEBAND METRICS
-------
CREATE EXTERNAL TABLE IF NOT EXISTS agg_accesses_timeband(
user_type string,
age tinyint,
gender char(1),
state string,
region string,
platform string,
timeband string,
accesses bigint,
accesses_with_views bigint,
accesses_without_views bigint
  )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_timeband';


-------
--TIMEBAND AGGREGATION JOB
-------

INSERT INTO TABLE agg_accesses_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	if(gender is null, 'GUEST', 'REGISTERED') AS user_type,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) AS age,
	gender,
	state,
	regionname AS region,
	platform,
	timeband,
	sum(unique_sessions) AS accesses,
	sum(accesses_with_views) AS accesses_with_views,
	sum(accesses_without_views) AS accesses_without_views
FROM(
    SELECT  
		if(login_devices.deviceid is null, loadhome.userid, login_devices.userid) AS userid,
		loadhome.deviceid AS deviceid,
		count(distinct loadhome.sessionid) AS unique_sessions,
		platform,
		 timeband,
		-- if(loadhome.userid = login_devices.userid, 'N', 'Y')  AS access_as_anonymous,
		sum(if(videostart.deviceid is null, 0,1)) AS accesses_with_views,
		sum(if(videostart.deviceid is null, 1,0)) AS accesses_without_views
	FROM(
		SELECT          
			userid, 
			deviceid,
			sessionid,
			customerid,
			platform,
			min(CASE WHEN substr(timestamp,9,2) between 0 and 3 THEN '0to4' 
			WHEN substr(timestamp,9,2) between 4 and 7 THEN '4to8'
			WHEN substr(timestamp,9,2) between 8 and 11 THEN'8to12'
			WHEN substr(timestamp,9,2) between 12 and 15 THEN '12to16'
			WHEN substr(timestamp,9,2) between 16 and 19 THEN'16to20'
			ELSE '20to24' END)  AS  timeband 
		FROM user_action 
		WHERE partition_date = '${hiveconf:ENDDATE}' -- specify target day
		AND eventtype = 'LOADHOME'  -- select "access platform" entries
		AND terminationreason = '' -- delect successful accesses
		GROUP BY 
			userid, 
			deviceid,
			sessionid,
			customerid,
			platform
			)loadhome
		LEFT OUTER JOIN( 
		SELECT 
			deviceid,
			collect_list(userid)[0]  AS userid
		FROM  login_devices
		GROUP BY deviceid
		)  login_devices
		ON  login_devices.deviceid = loadhome.deviceid	 
		LEFT OUTER JOIN (
		SELECT
			sessionid,
			deviceid
		FROM user_action 
		WHERE partition_date = '${hiveconf:ENDDATE}'
		AND eventtype = 'VIDEOSTART' --select entries related to content view
		AND contentsubtype != 'TRAILER'
		GROUP BY 
			sessionid,
			deviceid
		)videostart
		ON  loadhome.deviceid = videostart.deviceid -- merge 'LOADHOME' with 'VIDEOSTART' on sessionid and deviceid
		AND loadhome.sessionid =  videostart.sessionid 
  GROUP BY 
  loadhome.deviceid, loadhome.userid,
  login_devices.userid, login_devices.deviceid, 
  platform, timeband
	)viewership
LEFT OUTER JOIN profiling  ON profiling.userid = viewership.userid
    GROUP BY 
	gender,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state,
	regionname,
	platform,
	timeband;
	
-------
---SCHEMA FOR WEEKLY  METRICS
-------
CREATE TABLE IF NOT EXISTS agg_accesses_weekly(
user_type string,
age tinyint,
gender char(1),
state string,
region string,
platform string,
accesses bigint,
accesses_with_views bigint,
accesses_without_views bigint,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_weekly';


-------
-- WEEKLY AGGR.
-------
INSERT OVERWRITE TABLE agg_accesses_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT 
	user_type,
	age,
 	gender,
	state,
	region,
	platform,
	sum(accesses) as accesses,
	sum(accesses_with_views) as accesses_with_views,
	sum(accesses_without_views) as accesses_without_views,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp( '${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM agg_accesses_daily 
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
user_type,
platform,
age,
gender,
state,
region;

	
-------
-- SCHEMA  FOR MONTHLY  METRICS
-------

CREATE TABLE IF NOT EXISTS agg_accesses_monthly(
user_type string,
age tinyint,
gender char(1),
state string,
region string,
platform string,
accesses bigint,
accesses_with_views bigint,
accesses_without_views bigint,
partition_date string
  )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_monthly';

-----
--- MONTHLY AGGR.
-----

INSERT OVERWRITE TABLE agg_accesses_monthly PARTITION (month = '${hiveconf:YYYYMM}')
SELECT 
	user_type,
	age,
 	gender,
	state,
	region,
	platform,
	sum(accesses) as accesses,
	sum(accesses_with_views) as accesses_with_views,
	sum(accesses_without_views) as accesses_without_views,
	'${hiveconf:ENDDATE}' as partition_date
FROM agg_accesses_daily 
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
user_type,
platform,
age,
gender,
state,
region;
