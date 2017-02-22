--table created to store active users ON daily basis. goal - reduce further complexity
INSERT INTO TABLE watching_customer_base
PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT  
userid,
customerid
FROM watching
WHERE  partition_date = '${hiveconf:ENDDATE}'
AND download != 'Y'
group by userid, customerid;

-------  list contains all user ids and their start day
INSERT INTO TABLE watching_users_list
SELECT  new.* FROM (
	SELECT  user_id, 
	customerid, 
	partition_date AS start_date
	FROM watching_customer_base
	WHERE partition_date = '${hiveconf:ENDDATE}' 
)new
LEFT OUTER JOIN watching_users_list existed
on (existed.user_id = new.user_id) 
WHERE existed.user_id is null;

-----
-- Daily A_316 - A_219 
----
INSERT INTO TABLE agg_loyalty_daily  PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	user_type,
	state,
	region,
	count(case when (days >= '2'   AND  active_on_second_day = '1'  AND active_on_third_day = '1') 
          THEN loyalty.user_id end) AS loyal, -- #A_318
	count(case when (days = '1' AND  active_on_first_day = '1' AND start_day != '${hiveconf:ENDDATE}') 
          THEN loyalty.user_id end) AS lost, --#A_316
	count(case when (days = '1' AND start_day = '${hiveconf:ENDDATE}' ) 
          THEN loyalty.user_id end) AS new, --#A_319
	count(case when (days = '2' AND active_on_second_day = '0') 
	  THEN loyalty.user_id end) AS reconnected --#A_317

FROM(
SELECT 
		3_days.user_id,
		3_days.customerid as user_type,
		days,
		array_contains(active_days, first_day) as active_on_first_day,
		array_contains(active_days, second_day) as active_on_second_day,
		array_contains(active_days, third_day) as active_on_third_day,
		start_day, 
		state, 
		regionname AS region 
FROM(
SELECT  
		user_id, 
		IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS customerid,
		COUNT(user_id) AS days,
        COLLECT_SET(date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0)) as active_days,
		date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),2) AS first_day,
        date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),1) as  second_day,
    	date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),0) as  third_day,
    	COUNT(DISTINCT partition_date) as date_count
	FROM
	watching_customer_base
	WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
	between date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),2) and   
	from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')) --3 days of data selecteded
	GROUP BY user_id, customerid
    )3_days
LEFT OUTER JOIN profiling prof 
ON prof.userid = 3_days.user_id
LEFT OUTER JOIN watching_users_list w_list
ON  w_list.user_id = 3_days.user_id  WHERE w_list.start_day <= '${hiveconf:ENDDATE}'
    )loyalty
GROUP BY state, region, user_type;

----
-- user aging related KPIs:
-- Users who made Last View in Period -3/ -4/ <- 4
---

INSERT INTO TABLE loyalty_user_aging PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT  
	user_aging, 
	user_type,
	region,	
	state,
	COUNT(DISTINCT all_users.userid) AS distinct_users
FROM (
	SELECT  
		COALESCE(last_view.user_id, list.user_id) AS userid, 
		IF(COALESCE(last_view.customerid, list.customer_id) = 'GUEST', 'GUEST', 'REGISTERED') AS user_type, --rename customerid
		CASE WHEN (last_view = 4_day_break)THEN 'period_4' 
			WHEN (last_view is null) THEN 'more_than_4' 
			ELSE 'period_3' END AS user_aging --variable created based ON user action for latest 4 days
			--if user didn't make any action in last 4 days => last_view is null in SELECT ed 4 day timeframe
			--if last action appeared ON day equal  to 4_day_break - 4 days  since target day THEN 'period_4' 
	FROM (
		SELECT  
			user_id, 
			customerid, 
			MAX(date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0))  AS last_view, -- max timestamp = last view
			date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),3) AS 4_day_break 
		FROM watching_customer_base 
		WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) 
			between date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),3) and   
			from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')) --4 partitions of data SELECT ed
		GROUP BY user_id, customerid 
		)last_view
	FULL OUTER JOIN 
	watching_users_list list
	ON list.user_id = last_view.user_id 
    )all_users
LEFT OUTER JOIN(
SELECT  
	userid,
	regionname AS region,	
	state
FROM profiling) loc
ON loc.userid = all_users.userid --location data FROM profiling table added based ON key  = userid
GROUP BY  
	user_type,
	user_aging,
	region,	
	state; 

----
-- user aging related KPIs:
-- Users who made Last View in Period -3/ -4/ <- 4, Period = Week
---

INSERT OVERWRITE TABLE loyalty_user_aging_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT  
	user_aging, 
	user_type,
	region,	
	state,
	COUNT(DISTINCT all_users.userid) AS distinct_users,
	'${hiveconf:ENDDATE}' AS partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) AS week
FROM (
	SELECT  
		COALESCE(last_view.user_id, list.user_id) AS userid, 
		IF(COALESCE(last_view.customerid, list.customer_id) = 'GUEST', 'GUEST', 'REGISTERED') AS user_type, --rename customerid
	    CASE 
			WHEN weekofyear(last_view.last_view)  = last_view.4_period THEN 'period 4' -- last activity in week -4
   			WHEN (last_view is null) THEN 'more_than_4'  -- last activity before week -4
			ELSE 'period_3' 
			END AS user_aging --last activity between weeks -1 and -3
	FROM (
		SELECT  
			user_id, 
			customerid, 
			MAX(date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0))  AS last_view, -- max timestamp = last view
			--date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),27) AS 7_day_4_period, 
			weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),21)) AS 4_period  
		FROM watching_customer_base 
		WHERE weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}','yyyyMMdd')))
			BETWEEN weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),21))
			AND weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))      
		GROUP BY user_id, customerid 
		)last_view
	FULL OUTER JOIN 
	watching_users_list list
	ON list.user_id = last_view.user_id 
    )all_users
LEFT OUTER JOIN(
SELECT  
	userid,
	regionname AS region,	
	state
FROM profiling) loc
ON loc.userid = all_users.userid --location data FROM profiling table added based ON key  = userid
GROUP BY  
	user_type,
	user_aging,
	region,	
	state;


----
-- user aging related KPIs:
-- Users who made Last View in Period -3/ -4/ <- 4. period = Month
---

INSERT OVERWRITE TABLE loyalty_user_aging_monthly PARTITION (month = '${hiveconf:YYYYMM}')
SELECT  
	user_aging,
	user_type,
	region,
	state,
	COUNT(DISTINCT all_users.userid) AS distinct_users,
	'${hiveconf:ENDDATE}' AS partition_date
FROM
  (SELECT  COALESCE(last_view.user_id, list.user_id) AS userid,
    IF(COALESCE(last_view.customerid, list.customer_id) = 'GUEST', 'GUEST', 'REGISTERED') AS user_type, --rename customerid
    CASE
      WHEN (last_view.last_view BETWEEN last_view.first_day_4_period AND last_view.last_day_4_period)
      THEN 'period 4' -- last activity in month -4
      WHEN (last_view IS NULL)
      THEN 'more_than_4' -- last activity before month -4
      ELSE 'period_3'
    END AS user_aging --last activity between month -1 and -3
  FROM
    (SELECT  user_id
      ,customerid
      ,MAX(date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0)) AS last_view -- max timestamp = last view
	  , date_sub(from_unixtime(unix_timestamp(sd.startDate, 'yyyyMMdd')),0)  AS first_day_4_period
	  , date_sub(from_unixtime(unix_timestamp(sd.endDateStartMonth, 'yyyyMMdd')),0) AS last_day_4_period
    FROM watching_customer_base
    JOIN
      (SELECT 
        CASE
          WHEN monthYear.minus3Month < 10
          THEN CONCAT ( monthYear.startYear ,'0' ,monthYear.minus3Month ,'01' )
          ELSE CONCAT ( monthYear.startYear ,monthYear.minus3Month ,'01' )
        END AS startDate,
		CASE
          WHEN monthYear.minus3Month < 10
          THEN CONCAT ( monthYear.startYear ,'0' ,monthYear.minus3Month ,'31' )
          ELSE CONCAT ( monthYear.startYear ,monthYear.minus3Month ,'31' )
        END AS endDateStartMonth
      FROM
        (SELECT 
          CASE
            WHEN MONTH(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) - 3 < 0
            THEN 12                                                                      + (MONTH(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) - 3)
            WHEN MONTH(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) - 3 = 0
            THEN 12
            ELSE MONTH(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) - 3
          END AS minus3Month ,
          CASE
            WHEN MONTH(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) - 3 <= 0
            THEN YEAR(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))  - 1
            ELSE YEAR(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
          END AS startYear
        --FROM watching_customer_base LIMIT 1
        ) monthYear
      ) sd
    WHERE from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')) BETWEEN from_unixtime(unix_timestamp(sd.startDate, 'yyyyMMdd')) AND from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')) --4 partitions of data SELECT ed
    GROUP BY user_id,
      customerid,
	  startDate,
	  endDateStartMonth
    )last_view
  FULL OUTER JOIN watching_users_list list
  ON list.user_id = last_view.user_id
  )all_users
LEFT OUTER JOIN
  ( SELECT  userid, regionname AS region, state FROM profiling
  ) loc
ON loc.userid = all_users.userid --location data FROM profiling table added based ON key  = userid
GROUP BY user_type,
  user_aging,
  region,
  state;
  
 ----
 -- Weekly aggregations
 ----
  
INSERT OVERWRITE TABLE agg_loyalty_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT 
	user_type,
	state,
	region,
	COUNT(CASE WHEN (weeks_of_activity >= '2'   AND  active_on_second_week = '1'  AND active_on_third_week = '1') 
		THEN loyalty.user_id END) AS loyal, -- #A_318
	COUNT(CASE WHEN (weeks_of_activity = '1' AND  active_on_first_week = '1' AND start_week != current_week ) 
		THEN loyalty.user_id END) AS lost, --#A_316
	COUNT(CASE WHEN (weeks_of_activity = '1' AND start_week = current_week ) 
		THEN  loyalty.user_id END) AS new, --#A_319 
	COUNT(CASE WHEN (weeks_of_activity = '2' AND active_on_second_week = '0' ) 
		THEN loyalty.user_id END) AS reconnected, --#A_317
	'${hiveconf:ENDDATE}' AS partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) AS week
FROM(
	SELECT  
		3_weeks.user_id,
		user_type,
		weeks_of_activity,
		array_contains(active_weeks, first_week) as active_on_first_week,
		array_contains(active_weeks, second_week) as active_on_second_week,
		array_contains(active_weeks, third_week) as active_on_third_week,
		weekofyear(from_unixtime(unix_timestamp(start_day, 'yyyyMMdd'))) as start_week,
		weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) AS current_week,
		state,
		regionname AS region
	FROM (
		SELECT  
			user_id, 
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
			COLLECT_SET(weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')))) as active_weeks,
			COUNT(distinct weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')))) AS weeks_of_activity,
			weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as third_week,
			weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),7)) as second_week,
			weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),14)) as first_week
			FROM watching_customer_base
			WHERE 
				weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) in (
					weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))),
                                        weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),7)),
					weekofyear(date_sub(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),14))
				)
			AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
			GROUP BY 
			user_id, 
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED') 
		)3_weeks
	LEFT OUTER JOIN watching_users_list all_users
	ON 3_weeks.user_id = all_users.user_id
	LEFT OUTER JOIN profiling loc
	ON  loc.userid = 3_weeks.user_id 
	AND all_users.user_id = loc.userid
)loyalty
group by user_type, state, region;

---
-- monthly aggr.  A_316, A_317, A_318,A_319
---
INSERT OVERWRITE TABLE agg_loyalty_monthly PARTITION (month = '${hiveconf:YYYYMM}')
SELECT 
	user_type,
	state,
	region,
	COUNT(CASE WHEN (months_of_activity >= '2'   AND  active_on_second_month = '1'  AND active_on_third_month = '1') 
		THEN loyalty.user_id END) AS loyal, -- #A_318
	COUNT(CASE WHEN (months_of_activity = '1' AND  active_on_first_month = '1' AND start_month != current_month ) 
		THEN loyalty.user_id END) AS lost, --#A_316
	COUNT(CASE WHEN (months_of_activity = '1' AND start_month = current_month ) 
		THEN  loyalty.user_id END) AS new, --#A_319 
	COUNT(CASE WHEN (months_of_activity = '2' AND active_on_second_month = '0' ) 
		THEN loyalty.user_id END) AS reconnected, --#A_317
	'${hiveconf:ENDDATE}' AS partition_date
FROM(
	SELECT  
		3_months.user_id,
		user_type,
		months_of_activity,
		array_contains(active_months, first_month) as active_on_first_month,
		array_contains(active_months, second_month) as active_on_second_month,
		array_contains(active_months, third_month) as active_on_third_month,
		month(from_unixtime(unix_timestamp(start_day, 'yyyyMMdd'))) as start_month,
		month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) AS current_month,
		state,
		regionname AS region
	FROM (
			SELECT  
			user_id, 
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED')  AS user_type,
			COLLECT_SET(month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')))) as active_months,
			COUNT(distinct month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')))) AS months_of_activity,
			month(add_months(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),-2)) as first_month,
			month(add_months(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),-1)) as second_month,
			month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as third_month
			FROM watching_customer_base
			WHERE month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) in 
				(
				month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))),
				month(add_months(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),-1)),
				month(add_months(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),-2))
				)
			AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
			GROUP BY 
			user_id, 
			IF(customerid = 'GUEST', 'GUEST', 'REGISTERED') 
		)3_months
	LEFT OUTER JOIN watching_users_list all_users
	ON 3_months.user_id = all_users.user_id
	LEFT OUTER JOIN profiling loc
	ON  loc.userid = 3_months.user_id 
	AND all_users.user_id = loc.userid
)loyalty
group by user_type, state, region;
