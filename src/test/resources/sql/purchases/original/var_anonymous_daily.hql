---
--table structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS var_anonymous_base( 
userid	string,
appversion	string,	
platform	string,
accesses	int,	
playback_duration	decimal(15,4),
views	int,
channel_name	string,
rank_channel	int,
categoryname	string)
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/var_anonymous_base';

---
--required to collect watched categories and channels for F_8, F_9, G_8, G_9, F_25, F_26,.. KPIs
---
INSERT INTO TABLE var_anonymous_base PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT
	userid,
	appversion,	
	platform,
	accesses,
	playback_duration,
	sum(views) as views,
	collect_set(channel_name)[0] AS channel_name,
	RANK() OVER (PARTITION BY userid,appversion, platform ORDER BY playback_duration DESC) AS rank_channel,
	collect_set(categoryname)[0] AS categoryname
FROM(
	SELECT
		accesses.userid,
		accesses.appversion,	
		accesses.platform,
		sum(distinct accesses) as accesses,
		if(channel_name is not null, sum(playback_duration)+0.001, sum(playback_duration))  as playback_duration,
		sum(views) as views,
		channel_name,
		categoryname
	FROM(
		SELECT
			userid,
			appversion,	
			platform,	
			COUNT(distinct sessionid) as accesses
		FROM user_action
		WHERE partition_date='${hiveconf:ENDDATE}'  
		AND customerid = 'GUEST'
		GROUP BY 
			userid,
			appversion,	
			platform
    		)accesses
		LEFT OUTER JOIN (
		SELECT
			userid,
			appversion,
			platform,
			contentid,
			count(contentid) AS views,
			SUM(consumption)/60 AS playback_duration
		FROM watching
		WHERE partition_date='${hiveconf:ENDDATE}' and customerid = 'GUEST'
		GROUP BY 
			userid,
			appversion,
			platform,
			contentid
		)watching
	ON watching.userid = accesses.userid
	AND watching.platform = accesses.platform
	AND watching.appversion = accesses.appversion
	LEFT OUTER JOIN  vod_catalog vod ON watching.contentid = vod.contentid
	LEFT OUTER JOIN  tvchannels on  tvchannels.channel_id =  watching.contentid
	GROUP BY 
		accesses.userid,
		accesses.appversion,	
		accesses.platform,
		channel_name,
		categoryname
	)content
GROUP BY  
	userid,  categoryname, channel_name,
	appversion, 
	platform, 
	playback_duration,
	accesses;

---
--table structure for VAR_anonymous daily
---
CREATE EXTERNAL TABLE IF NOT EXISTS var_anonymous_1d( 
userid	string,
appversion	string,
platform string,
accesses int,
views int,
playback_duration	decimal(15,4),
live_views	int,
live_playback_duration	decimal(15,4), 
video_views	int,
video_playback_duration decimal(15,4),
top_watched_channel string,
top_watched_category	string,
first_view_date	string,
first_access_date string)
PARTITIONED BY (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/var_anonymous_1d';


INSERT INTO TABLE  var_anonymous_1d PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	viewership.*,
	channel_name as top_watched_channel,
	categoryname as top_watched_category,
	w.start_day as first_view_date,
	a.access_day as first_access_date
FROM (
	SELECT
		userid,
		appversion,
		platform,
		sum(accesses) as accesses,
		sum(views)as views,
		sum(playback_duration) as playback_duration,
		sum(if(channel_name is not NULL, views,0)) as live_views, --??
		sum(if(channel_name is not NULL, playback_duration,0)) as live_playback_duration, --??
		sum(if(categoryname is not NULL, views,0)) as video_views,
		sum(if(categoryname is not NULL, playback_duration,0)) as video_playback_duration
	FROM var_anonymous_base
	WHERE partition_date='${hiveconf:ENDDATE}'
	GROUP BY  
		userid,
		appversion,
		platform
	)viewership
	LEFT OUTER JOIN (
	SELECT * from (
		SELECT 
		userid,
		appversion,
		platform,
		playback_duration,
		collect_set(channel_name)[0] AS channel_name,
		RANK() OVER (PARTITION BY userid,appversion, platform ORDER BY playback_duration DESC) AS rank_
	FROM var_anonymous_base
	WHERE  partition_date = '${hiveconf:ENDDATE}' 
	AND channel_name is not NULL
	GROUP BY 
		userid,
		appversion,
		platform,
		playback_duration
		)rank_channel
	WHERE rank_channel.rank_ = 1
	)top_channel
	ON top_channel.userid = viewership.userid
	AND top_channel.appversion = viewership.appversion
	AND top_channel.platform = viewership.platform
LEFT OUTER JOIN (
SELECT * FROM (
	SELECT
		userid,
		appversion,
		platform,
		playback_duration,
		collect_set(categoryname)[0] AS categoryname,
		RANK() OVER (PARTITION BY userid,appversion, platform ORDER BY playback_duration DESC) AS rank_
	FROM var_anonymous_base
	WHERE partition_date = '${hiveconf:ENDDATE}' 
	AND categoryname is not NULL
	GROUP  BY
		userid,
		appversion,
		platform,
		playback_duration
	)rank_category
WHERE rank_category.rank_ = 1
)top_category
ON top_category.userid = viewership.userid
AND top_category.appversion = viewership.appversion
AND top_category.platform = viewership.platform

LEFT OUTER JOIN watching_users_list w ON w.user_id = viewership.userid
LEFT OUTER JOIN access_users_list a ON a.user_id = viewership.userid;
