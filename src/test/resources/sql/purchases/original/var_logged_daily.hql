CREATE EXTERNAL TABLE IF NOT EXISTS var_logged_base( 
userid string,
number_used_platform tinyint,
number_used_devices tinyint,
accesses int,
playback_duration decimal(15,4),
views int,
downloads int,
channel_name string,
rank_channel int,
categoryname string,
platform string)
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/var_logged_base';

INSERT INTO TABLE var_logged_base PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT
    userid,	
	number_used_platform,
	number_used_devices,
	accesses,
	playback_duration,
	sum(views) as views,
	sum(downloads) as downloads,
	collect_set(channel_name)[0] AS channel_name,
	RANK() OVER (PARTITION BY userid ORDER BY playback_duration DESC) AS rank_channel,
	collect_set(categoryname)[0] AS categoryname,
	collect_set(platform)[0] AS platform
FROM(
	SELECT
		COALESCE(accesses.userid, watching.userid) as userid,
		sum(distinct number_used_platform) as number_used_platform,
		sum(distinct accesses) as accesses,
		sum(distinct number_used_devices) as number_used_devices,
		if(channel_name is not null, sum(playback_duration)+0.001,sum(playback_duration))  as playback_duration,
		sum(views) as views,
		sum(downloads) as downloads,
		channel_name,
		categoryname,
		platform
	FROM(

		SELECT
			userid,
			COUNT(distinct platform) as number_used_platform,	
			COUNT(distinct sessionid) as accesses,
			COUNT(distinct deviceid) as number_used_devices
		FROM login
		WHERE partition_date='${hiveconf:ENDDATE}'  
		AND eventtype = 'LOGIN'
		AND loginsuccess = 'Y'
		GROUP BY 
			userid
    		)accesses
			
		FULL OUTER JOIN (	
			
		SELECT
			userid,
			platform,
			contentid,
			COUNT(CASE WHEN download != 'Y' THEN contentid END) AS views,
			SUM(CASE WHEN download != 'Y' THEN consumption END)/60 as playback_duration, 
			SUM(IF(download = 'Y',1,0)) AS  downloads
		FROM watching
		WHERE partition_date='${hiveconf:ENDDATE}' and customerid != 'GUEST'
		GROUP BY 
			userid, 
			platform,
			contentid
		)watching	
	ON watching.userid = accesses.userid
	LEFT OUTER JOIN  vod_catalog vod ON watching.contentid = vod.contentid
	LEFT OUTER JOIN  tvchannels on  tvchannels.channel_id =  watching.contentid
	GROUP BY 
		COALESCE(accesses.userid, watching.userid),
		platform,
		channel_name,
		categoryname
	)content
GROUP BY  
	userid,  categoryname, channel_name,
	number_used_platform,
	number_used_devices,
	playback_duration,
	accesses;
	
--====
-- DAILY VAR SCHEMA
--====
CREATE EXTERNAL TABLE IF NOT EXISTS var_logged_1d (
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
revenues decimal(15,2))
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/var_logged_1d';

--====
-- DAILY VAR AGGR
--====

INSERT INTO TABLE  var_logged_1d PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	COALESCE(viewership.userid, purchases.userid) AS userid,
	birth_date,
	gender,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	state,
	province,
	city,
	zip_code,
	registrationmethod,
	referralsource,
	registrationtrigger,
	j.date as registrationdate,
	most_used_platform,
	a.access_day as first_access_date,
	w.start_day as first_view_date,
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
	revenues

FROM (
	SELECT
		userid,
		avg(accesses) as accesses,
		sum(downloads) as downloads,
		sum(views)as views,
		sum(playback_duration) as playback_duration,
		sum(if(channel_name is not NULL, views,0)) as live_views, --??
		sum(if(channel_name is not NULL, playback_duration,0)) as live_playback_duration, --??
		sum(if(categoryname is not NULL, views,0)) as video_views,
		sum(if(categoryname is not NULL, playback_duration,0)) as video_playback_duration,
		number_used_platform,
		number_used_devices
	FROM var_logged_base
	WHERE partition_date='${hiveconf:ENDDATE}'
	GROUP BY  
		userid, 
		number_used_platform,
		number_used_devices
	)viewership
	
	LEFT OUTER JOIN (
	SELECT * from (
		SELECT 
		userid,
		collect_set(platform)[0] AS most_used_platform,
		RANK() OVER (PARTITION BY userid ORDER BY playback_duration DESC) AS rank_
	FROM var_logged_base
	WHERE  partition_date = '${hiveconf:ENDDATE}' 
	GROUP BY 
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
	FROM var_logged_base
	WHERE partition_date = '${hiveconf:ENDDATE}' 
	AND categoryname is not NULL
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
	FROM var_logged_base
	WHERE  partition_date = '${hiveconf:ENDDATE}' 
	AND channel_name is not NULL
	GROUP BY 
		userid,
		playback_duration
		)rank_channel
	WHERE rank_channel.rank_ = 1
	)top_channel
ON top_channel.userid = viewership.userid
FULL OUTER JOIN (	
	SELECT 
		userid,
		SUM(CASE WHEN discountedprice>0 THEN discountedprice ELSE originalprice END) AS revenues,
		COUNT(userid) AS purchases 
	FROM purchase
	WHERE partition_date = '${hiveconf:ENDDATE}'
	GROUP BY  
	userid
    )purchases
ON purchases.userid =  viewership.userid

LEFT OUTER JOIN watching_users_list w ON w.user_id = COALESCE(viewership.userid, purchases.userid)
LEFT OUTER JOIN login_users_list a ON a.user_id = COALESCE(viewership.userid, purchases.userid)
LEFT OUTER JOIN profiling ON profiling.userid = COALESCE(viewership.userid, purchases.userid)
LEFT OUTER JOIN join_users_list j ON j.uniquecontract = COALESCE(viewership.userid, purchases.userid);