--====
-- VAR  LOGGED SCHEMA ${hiveconf:FRAME}D
--====
CREATE TABLE IF NOT EXISTS var_logged_${hiveconf:FRAME}D(
userid string,
birth_date string, 
gender char(1),
age string,
state string,
province string,
city string,
zip_code string,
registrationmethod string,
referralsource string,
registrationtrigger string,
registrationdate string,
most_used_platform string,    
first_access_date string,
first_view_date string,
accesses int,
number_used_platform int,
number_used_devices int,
video_views int,
video_playback_duration decimal(15,4),
live_views int,
live_playback_duration decimal(15,4),
views int,
playback_duration decimal(15,4),
top_watched_category string,
top_watched_channel string,
downloads int,
purchases int,
revenues decimal(15,2),
partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/var_logged_${hiveconf:FRAME}D';

--===
--  VAR LOGED ${hiveconf:FRAME}D AGGR. where FRAME = 7/30/60/90
--===
INSERT OVERWRITE TABLE var_logged_${hiveconf:FRAME}d
SELECT 
	viewership.userid,
	birth_date,
	gender,
	age,
	state,
	province,
	city,
	zip_code,
	registrationmethod,
	referralsource,
	registrationtrigger,
	j.date as registrationdate,
	most_used_platform,
	first_access_date,
	first_view_date,
	accesses,
	number_used_platform,
	number_used_devices,
	video_views,
	video_playback_duration,
	live_views,
	live_playback_duration,
	views,
	playback_duration,
	categoryname as top_watched_category,
	channel_name as top_watched_channel,
	downloads,
	purchases,
	revenues,
	'${hiveconf:ENDDATE}' as partition_date
FROM (
	SELECT
		userid,
		sum(accesses) as accesses,
		sum(downloads) as downloads,
		sum(views)as views,
		sum(playback_duration) as playback_duration,
		sum(live_views) as live_views, --??
		sum(live_playback_duration) as live_playback_duration, --??
		sum(video_views) as video_views,
		sum(video_playback_duration) as video_playback_duration,
		sum(purchases) as purchases,
		sum(revenues) as revenues,
		first_view_date,
		first_access_date,
		birth_date,
		max(age) as age,
		gender,
		state,
		province,
		city,
		zip_code
	FROM var_logged_1d
	WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
		BETWEEN date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),${hiveconf:FRAME}-1) and   
		from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))
	GROUP BY  
		userid,
		first_view_date,
		first_access_date,
		birth_date,
		gender,
		state,
		province,
		city,
		zip_code
	)viewership
	
	LEFT OUTER JOIN (
	SELECT * from (
	SELECT
		userid,
		collect_set(platform)[0] AS most_used_platform,
		RANK() OVER (PARTITION BY userid ORDER BY playback_duration DESC) AS rank_
	FROM(
		SELECT 
			userid, 
			platform,
			sum(playback_duration)  as playback_duration
		FROM var_logged_base
		WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
		BETWEEN date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),${hiveconf:FRAME}-1) and   
		from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))
		GROUP BY  
        userid, 
        platform
    )platform_cons
	GROUP  BY
		userid,
		playback_duration
		)rank_platform
	WHERE rank_platform.rank_ = 1
	)top_platform
ON top_platform.userid = viewership.userid
	
LEFT OUTER JOIN (
SELECT * FROM (
	SELECT
		userid,
		collect_set(categoryname)[0] AS categoryname,
		RANK() OVER (PARTITION BY userid ORDER BY playback_duration DESC) AS rank_
	FROM(
		SELECT 
			userid, 
			categoryname,
			sum(playback_duration)  as playback_duration
		FROM var_logged_base
		WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
		BETWEEN date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),${hiveconf:FRAME}-1) and   
		from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))
		AND categoryname is not NULL
		GROUP BY  
        userid, 
        categoryname
	)category_cons
	GROUP  BY
		userid,
		playback_duration
	)rank_category
WHERE rank_category.rank_ = 1
)top_category
ON top_category.userid = viewership.userid
	LEFT OUTER JOIN (
	SELECT * from (
	SELECT
		userid,
		collect_set(channel_name)[0] AS channel_name,
		RANK() OVER (PARTITION BY userid ORDER BY playback_duration DESC) AS rank_
	FROM(
		SELECT 
			userid, 
			channel_name,
			sum(playback_duration)  as playback_duration
		FROM var_logged_base
		WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
		BETWEEN date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),${hiveconf:FRAME}-1) and   
		from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))
		AND channel_name is not NULL
		GROUP BY  
		userid, 
		channel_name
		)channel_cons
		GROUP  BY
			userid,
			playback_duration
			)rank_channel
		WHERE rank_channel.rank_ = 1
		)top_channel
ON top_channel.userid = viewership.userid
LEFT OUTER JOIN(
SELECT
userid,
COUNT( DISTINCT platform) as number_used_platform,
COUNT( DISTINCT deviceid) as number_used_devices
FROM login
WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
		BETWEEN date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),${hiveconf:FRAME}-1) and   
		from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))
AND eventtype = 'LOGIN'
AND loginsuccess = 'Y'
GROUP BY userid
)unique_dev
ON unique_dev.userid = viewership.userid
LEFT OUTER JOIN join_users_list j ON j.uniquecontract = viewership.userid;
