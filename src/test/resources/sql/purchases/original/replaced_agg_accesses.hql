---
-- table timeband structure
---

CREATE EXTERNAL TABLE IF NOT EXISTS agg_accesses_timeband( 
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
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_timeband';

---
-- timeband aggegations
--A_28,A_29, A_30, A_31, A_50
--

INSERT INTO TABLE agg_accesses_timeband PARTITION  (partition_date = '${hiveconf:ENDDATE}')
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

CREATE EXTERNAL TABLE IF NOT EXISTS agg_accesses_daily( 
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
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_daily';

-----
-- DAILY A_28, A_29, A_50, A_30, A_31, A_21
-----

INSERT INTO TABLE agg_accesses_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
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
CREATE TABLE IF NOT EXISTS agg_accesses_weekly( 
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
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_weekly';

-----
-- WEEKLY  A_28, A_29, A_50, A_30, A_31, A_21
-----
INSERT OVERWRITE TABLE agg_accesses_weekly PARTITION (year = '${hiveconf:YEAR}', week)
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
FROM agg_accesses_daily --aggregations based on daily pre aggregates.
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
CREATE TABLE IF NOT EXISTS agg_accesses_monthly( 
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
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_accesses_monthly';

-----
-- MONTHLY  A_28, A_29, A_50, A_30, A_31, A_21
-----
INSERT OVERWRITE TABLE agg_accesses_monthly PARTITION (month = '${hiveconf:YYYYMM}')
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
FROM agg_accesses_daily --aggregations based on daily pre aggregates.
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
	
---
--table structure for  daily A_26,A_27 + "Anonymous/All Accesses" variables
--(A_20,A_21,A_22,A_23,A_24,A_25)
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_with_without_view_daily ( 
user_type string,
platform string,
age tinyint,
gender char(1),
state string,
region string,
accesses int,
accesses_without_view int,
accesses_with_view int)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_with_without_view_daily';


INSERT INTO TABLE accesses_with_without_view_daily  PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	user_type, 
	platform,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
	state,
	regionname AS region,
	COUNT(loadhome.userid) AS accesses,
	SUM(IF(videostart.userid is null, 1,0)) AS accesses_without_view,  --sum of accesses without content view.
	SUM(IF(videostart.userid is not null, 1,0)) AS  accesses_with_view --sum of accesses followed by at least one content view.
FROM (
	SELECT 
		sessionid,
		userid, 
		IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type, --set user_type variable
		platform
	FROM user_action
	WHERE partition_date = '${hiveconf:ENDDATE}' --specify target day
	AND eventtype = 'LOADHOME'  --select "access platform" entries
	AND terminationreason = '' --select successful accesses
	GROUP BY 
		sessionid, 
		customerid,
		userid, 
		platform
	)loadhome
LEFT OUTER JOIN (
	SELECT
		sessionid,
		userid
	FROM user_action 
	WHERE partition_date = '${hiveconf:ENDDATE}'
	AND eventtype = 'VIDEOSTART' --select entries related to content view
	GROUP BY 
		sessionid,
		userid
	)videostart
ON  loadhome.userid = videostart.userid --merge 'LOADHOME' with 'VIDEOSTART' on sessionid and userid
AND loadhome.sessionid =  videostart.sessionid 
LEFT OUTER JOIN profiling  --join location data
ON profiling.userid = loadhome.userid
GROUP BY  --group by required dimensions
	user_type, 
	platform, 
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
 	gender,
	state, 
	regionname;

---
-- Table schema for Timeband A_26 A_27
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_with_without_view_timeband( 
user_type string,
platform string,
age tinyint,
gender char(1),
state string,
region string,
timeband string,
accesses int,
accesses_without_view int,
accesses_with_view int)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_with_without_view_timeband';

---
-- Timeband aggr. 
---

INSERT INTO TABLE accesses_with_without_view_timeband  PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	user_type, 
	platform,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
	state,
	regionname AS region,
	timeband,
	COUNT(loadhome.userid) AS accesses,
	SUM(IF(videostart.userid is null, 1,0)) AS accesses_without_view,  --sum accesses without content view.
	SUM(IF(videostart.userid is not null, 1,0)) AS  accesses_with_view --amount of accesses followed by at least one content view.
FROM (
	SELECT 
		sessionid,
		userid, 
		IF(coalesce(customerid) = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type, --set user_type variable
		platform,
		min(CASE WHEN substr(timestamp,9,2) between 0 and 3 THEN '0to4' 
		WHEN substr(timestamp,9,2) between 4 and 7 THEN '4to8'
		WHEN substr(timestamp,9,2) between 8 and 11 THEN'8to12'
		WHEN substr(timestamp,9,2) between 12 and 15 THEN '12to16'
		WHEN substr(timestamp,9,2) between 16 and 19 THEN'16to20'
		ELSE '20to24' END)  AS  timeband   --Time slots of a day.
	FROM user_action
	WHERE partition_date = '${hiveconf:ENDDATE}' --specify target day
	AND eventtype = 'LOADHOME'  --select "access platform" entries
	AND terminationreason = '' --select successful accesses
	GROUP BY 
		sessionid, 
		customerid,
		userid, 
		platform
	)loadhome
LEFT OUTER JOIN (
	SELECT
		sessionid,
		userid
	FROM user_action 
	WHERE partition_date = '${hiveconf:ENDDATE}'
	AND eventtype = 'VIDEOSTART' --select entries related to content view
	GROUP BY 
		sessionid,
		userid
)videostart
ON  loadhome.userid = videostart.userid --merge 'LOADHOME' with 'VIDEOSTART' on sessionid and userid
AND loadhome.sessionid =  videostart.sessionid 
LEFT OUTER JOIN profiling  --join location data
ON profiling.userid = loadhome.userid
GROUP BY  --group by required dimensions
	user_type, 
	platform, 
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
 	gender,
	state, 
	regionname,
	timeband;

---
-- schema for weekly KPIs A_26, A_27 + "Accesses" 
---

CREATE TABLE IF NOT EXISTS accesses_with_without_view_weekly ( 
user_type string,
platform string,
age tinyint,
gender char(1),
state string,
region string,
accesses int,
accesses_without_view int,
accesses_with_view int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_with_without_view_weekly';

---
-- Weekly aggr.
---
INSERT OVERWRITE TABLE  accesses_with_without_view_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT 
	user_type,
	platform,
	age,
 	gender,
	state,
	region,
	sum(accesses) as accesses,
	sum(accesses_without_view) as accesses_without_view,
	sum(accesses_with_view) as accesses_with_view,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp( '${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM accesses_with_without_view_daily 
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
user_type,
platform,
age,
gender,
state,
region;

---
--table structure for monthly KPIs A_26,A_27
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_with_without_view_monthly ( 
user_type string,
platform string,
age tinyint,
gender char(1),
state string,
region string,
accesses int,
accesses_without_view int,
accesses_with_view int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_with_without_view_monthly';

---
--Monthly aggr.A_26,A_27
---
INSERT OVERWRITE TABLE  accesses_with_without_view_monthly PARTITION (month = '${hiveconf:YYYYMM}')
SELECT 
	user_type,
	platform,
	age,
 	gender,
	state,
	region,
	sum(accesses) as accesses,
	sum(accesses_without_view) as accesses_without_view,
	sum(accesses_with_view) as accesses_with_view,
	'${hiveconf:ENDDATE}' as partition_date
FROM accesses_with_without_view_daily  --based on daily aggregations
WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 
user_type,
platform,
age,
gender,
state,
region;
