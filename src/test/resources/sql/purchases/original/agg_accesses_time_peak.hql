---
--Structure for Daily A_35
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_maximum_time_peak_daily( 
	user_type string,
	platform string,
	state string,
	region string,
	user_concurrency int,
	timepeaks array<string> )
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_maximum_time_peak_daily';

---
--Daily A_35
---
INSERT INTO TABLE  accesses_maximum_time_peak_daily PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	user_type,
	platform,
	state,
	regionname AS region,
	user_concurrency,
	timepeaks
FROM (
	SELECT
		user_type,
		platform,
		state,
		regionname,
		user_concurrency,
		collect_set(timestamp) AS timepeaks, --collect all HHmm where peaks apears as an array
		RANK() OVER (PARTITION BY state,regionname, user_type,platform ORDER BY user_concurrency DESC) AS rank_ --rank over dimensions and collect corresponding concurrency
	FROM(
		SELECT
			user_type,
			platform,
			state,
			regionname,
			COUNT(userid) AS user_concurrency, --count all users/entries that have  eventtype = 'LOADHOME'
			timestamp
		FROM(
			SELECT
				platform,
				state, 
				regionname,
				IF(acc.customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type, --set user_type variable
				acc.userid, 
				SUBSTR(timestamp,9,4) AS timestamp --Timestamp  (HHmm) referred to the minute of the day in which the maximum number of accesses happened.
			FROM user_action acc
			LEFT OUTER JOIN --join location data
			profiling prof
			ON  prof.userid = acc.userid
			WHERE acc.partition_date = '${hiveconf:ENDDATE}'
			AND eventtype = 'LOADHOME'  --select "access platform" entries
			AND terminationreason = '' --select successful accesses
        )user_count
		GROUP BY timestamp, state, regionname, platform, user_type
    )collect_time
	GROUP BY user_concurrency, state, regionname, platform,  user_type
)rank_1
WHERE rank_1.rank_=1;

---
--Structure for weekly A_35
---
CREATE TABLE IF NOT EXISTS accesses_maximum_time_peak_weekly( 
user_type string,
platform string,
state string,
region string,
user_concurrency int,
date_peaks array<string>,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_maximum_time_peak_weekly';

---
--Weekly A_35
---
INSERT OVERWRITE  TABLE  accesses_maximum_time_peak_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT 
	user_type,
	platform,
	state,
	region,
	user_concurrency,
	date_peaks,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM(
	SELECT
		user_type,
		platform,
		state,
		region,
		user_concurrency,
		collect_set(partition_date) AS date_peaks, --collect all dates where peaks apears as an array
		RANK() over (partition by user_type, platform, state, region, user_type order by user_concurrency desc) AS rank_ --rank over dimensions to collect unique user_concurrency variables
	FROM accesses_maximum_time_peak_daily --aggregations based on daily aggr.
	WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type,platform, state, region, user_concurrency --group by dimensions and concurr.
)rank1
--entries corresponding to highest concurrency selected
WHERE rank1.rank_=1;

---
--Structure for monthly A_35
---
CREATE EXTERNAL TABLE IF NOT EXISTS accesses_maximum_time_peak_monthly( 
user_type string,
platform string,
state string,
region string,
user_concurrency int,
date_peaks array<string>
)
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/accesses_maximum_time_peak_monthly';

---
--Monthly A_35
---
INSERT INTO TABLE  accesses_maximum_time_peak_monthly PARTITION (partition_date = '${hiveconf:ENDDATE}') 
SELECT 
	user_type,
	platform,
	state,
	region,
	user_concurrency,
	date_peaks
FROM(
	SELECT
		user_type,
		platform,
		state,
		region,
		user_concurrency,
		collect_set(partition_date) AS date_peaks, --collect all dates where peaks apears as an array
		RANK() over (partition by user_type, platform, state, region, user_type order by user_concurrency desc) AS rank_ --rank over dimensions to collect unique user_concurrency variables
	FROM accesses_maximum_time_peak_daily --aggregations based on daily aggr.
	WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY user_type,platform, state, region, user_concurrency --group by dimensions and concurr.
)rank1
--entries corresponding to highest concurrency selected
WHERE rank1.rank_=1;
