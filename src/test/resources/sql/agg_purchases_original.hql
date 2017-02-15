---
-- purchases 1D KPIs schema
---
CREATE EXTERNAL TABLE IF NOT EXISTS agg_purchases_daily(
platform string,
age tinyint,
gender char(1),
appversion string,
paymenttype string,
currency string,
revenues decimal(15,2),
purchases int,
categoryname string,
contenttype string,
genre string,
channel string,
region string,
state string
    )
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_daily';

---
-- purchases related daily KPIs
--A_10,A_12,A_13,A_153,A_159,A_160,A_161,A_162,A_175,A_176,A_177,A_178
---

INSERT INTO TABLE agg_purchases_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	platform,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
 	gender,
	appversion,
	paymenttype,
	currency,
	SUM(revenues) AS revenues, --all revenues for for corresponding set of dimensions
	SUM(purchases) AS purchases,
	COALESCE(categoryname, channel_category) AS  categoryname,
	COALESCE(contenttype, channel_type) AS  contenttype,
	COALESCE(genre, channel_genre) AS  genre,
	channel_name as channel,
	regionname as region,
	state
FROM (
	SELECT 
		userid,
		platform,
		appversion,
		paymenttype,
		currency,
		contentid,
		contenttype,
		SUM(CASE WHEN discountedprice>0 THEN discountedprice ELSE originalprice END) AS revenues, --if no discount then original price is taken, otherwise discount price used for aggr.
		COUNT(userid) AS purchases 
	FROM purchase
	WHERE partition_date = '${hiveconf:ENDDATE}'
	GROUP BY  
	userid,contenttype, contentid,
	platform,appversion,paymenttype,currency
    )purch
LEFT OUTER JOIN vod_catalog content ON content.contentid = purch.contentid
LEFT OUTER JOIN tvchannels ON  tvchannels.channel_id =  purch.contentid
LEFT OUTER JOIN profiling ON profiling.userid = purch.userid

GROUP BY --group by all dimensions
platform, gender, 
floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
appversion,paymenttype,currency,
COALESCE(contenttype, channel_type),
COALESCE(categoryname, channel_category),
COALESCE(genre, channel_genre), 
channel_name,
regionname,state;

---
-- purchases 7D KPIs schema
---
CREATE TABLE IF NOT EXISTS agg_purchases_weekly(
platform string,
age tinyint,
gender char(1),
appversion string,
paymenttype string,
currency string,
revenues decimal(15,2),
purchases int,
categoryname string,
contenttype string,
genre string,
channel string,
region string,
state string,
partition_date string
    )
PARTITIONED BY (year string, week int) 
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_weekly';

---
-- purchases weekly KPIs
--A_10,A_12,A_13,A_153,A_159,A_160,A_161,A_162,A_175,A_176,A_177,A_178
---

INSERT OVERWRITE TABLE agg_purchases_weekly PARTITION (year = '${hiveconf:YEAR}', week)
SELECT 
	platform,
	age,
	gender,
	appversion,
	paymenttype,
	currency,
	SUM(revenues) AS revenues,
	SUM(purchases) AS purchases,
	categoryname,
	contenttype,
	genre,
	channel,
	region,
	state,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM agg_purchases_daily --aggr. based on daily preaggregated data.
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY  -- data of 7 days grouped by all dimensions
	platform, 
	age,
	gender,
	contenttype,
	appversion,
	paymenttype,
	currency,
	categoryname,
	genre,
	channel,
	region,
	state;  

---
-- purchases 30D KPIs schema
---
CREATE TABLE IF NOT EXISTS agg_purchases_monthly(
platform string,
age tinyint,
gender char(1),
appversion string,
paymenttype string,
currency string,
revenues decimal(15,2),
purchases int,
categoryname string,
contenttype string,
genre string,
channel string,
region string,
state string,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_monthly';

---
-- purchases Monthly KPIs
--A_10,A_12,A_13,A_153,A_159,A_160,A_161,A_162,A_175,A_176,A_177,A_178


INSERT OVERWRITE TABLE agg_purchases_monthly PARTITION  (month = '${hiveconf:YYYYMM}')
SELECT 
	platform,
	age,
	gender,
	appversion,
	paymenttype,
	currency,
	SUM(revenues) AS revenues,
	SUM(purchases) AS purchases,
	categoryname,
	contenttype,
	genre,
	channel,
	region,
	state,
	'${hiveconf:ENDDATE}' as partition_date
FROM agg_purchases_daily --based on daily pre-aggregated data
WHERE   month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY  --data grouped by dimensions
platform, age, gender, contenttype,
appversion,paymenttype,currency,
categoryname,genre, channel,
region,state; 

--=============================
--data preprocessing for weekly/monthly/yearly aggr
--goal - reduce processing time - instead of raw data aggr. to detect unique users
--this table - containing list of unique users and latest purchase day - can be used:
--=============================
---
--structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS purchase_user_list( 
userid string,
customerid string,
currency string,
latest_purchase_date string
)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/purchase_user_list';

---
--list of unique userid|customerid + day of latest purchase
---
--table contains information  about currency each user use, date when lates purchase was made - created to reduce complexity of further aggregations
--for ex. to detect amount of purchases by unique users without processing 90 days of raw data
INSERT OVERWRITE TABLE purchase_user_list 
SELECT
	userid,
	customerid,
	currency,
	MAX(latest_purchase_date) AS latest_purchase_date
FROM(
	SELECT
		userid,
		customerid,
		currency,
		date_sub(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd')),0) AS latest_purchase_date
	FROM  purchase  
	WHERE partition_date = '${hiveconf:ENDDATE}'
UNION  ALL
    SELECT * FROM purchase_user_list existed_purch
    )new_purchases
GROUP BY userid, customerid, currency;
--============================

---User Button Purchases
--
--table structure for daiy aggr
--
CREATE EXTERNAL TABLE IF NOT EXISTS agg_purchases_daily_user_button(
currency string,
age tinyint,
gender char(1),
revenues decimal(15,2),
purchases int,
region string,
state string,
users int,
average_purchases_per_user decimal(15,4)
    )
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_daily_user_button';

---
-- Purchases/ users button/ daily KPIs
-- A_164,A_167,A_168.A_169,A_170,A_171
---

INSERT INTO TABLE agg_purchases_daily_user_button PARTITION  (partition_date = '${hiveconf:ENDDATE}')
SELECT 
	currency,
	age,
	gender,
	SUM(revenues) AS revenues, --sum all revenues  that correspond to set of dimensions
	SUM(purchases) AS purchases, --sum  all purchases  that correspond to set of dimensions
	region,
	state,
	count(distinct purch.userid) AS users, --amount of unique users who made purchases
	SUM(purchases)/count(distinct purch.userid) AS average_purchases_per_user
FROM (
	SELECT 
		userid,
		currency,
		SUM(CASE WHEN discountedprice>0 THEN discountedprice ELSE originalprice END) AS revenues, --if no discount then original price is taken, otherwise discount price used for aggr.
		COUNT(userid) AS purchases
	FROM purchase
	WHERE partition_date = '${hiveconf:ENDDATE}' -- target day
    GROUP BY  
    userid,customerid,currency
    )purch
LEFT OUTER JOIN( --joined with location data
SELECT
    userid,
    floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
    gender,
    regionname as region,
    state
    FROM profiling
)profile
ON profile.userid = purch.userid

GROUP BY  
age, gender, currency,region,state;


---
--table structure
---
CREATE TABLE IF NOT EXISTS agg_purchases_weekly_user_button(
currency string,
age tinyint,
gender char(1),
revenues decimal(15,2),
purchases int,
region string,
state string,
users int,
average_purchases_per_user decimal(15,4),
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_weekly_user_button';

---
-- weekly KPIs/purchases/users button
--A_164,A_167,A_168.A_169,A_170,A_171
---

INSERT OVERWRITE TABLE agg_purchases_weekly_user_button PARTITION  (year = '${hiveconf:YEAR}', week)
SELECT 
	currency,
	age,
	gender,
	SUM(revenues) AS revenues, --sum all revenues  that correspond to set of dimensions
	SUM(purchases) AS purchases, --sum  all purchases  that correspond to set of dimensions
	region,
	state,
	count(distinct purch.userid) AS users, --amount of unique users who made purchases
	SUM(purchases)/count(distinct purch.userid) AS average_purchases_per_user,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM (
	SELECT 
		userid,
		currency,
		SUM(CASE WHEN discountedprice>0 THEN discountedprice ELSE originalprice END) AS revenues, --if no discount then original price is taken, otherwise discount price used for aggr.
		COUNT(userid) AS purchases
	FROM purchase
	WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
    GROUP BY  
    userid,customerid,currency
    )purch
LEFT OUTER JOIN( --joined with location data
SELECT
    userid,
    floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
    gender,
    regionname as region,
    state
    FROM profiling
)profile
ON profile.userid = purch.userid

GROUP BY  
age, gender, currency,region,state;


---
--schema for monthly KPIs
---
CREATE TABLE IF NOT EXISTS agg_purchases_monthly_user_button(
currency string,
age tinyint,
gender char(1),
revenues decimal(15,2),
purchases int,
region string,
state string,
users int,
average_purchases_per_user decimal(15,4),
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_purchases_monthly_user_button';

---
--Monthly aggr. A_164,A_167,A_168.A_169,A_170,A_171
---

INSERT OVERWRITE TABLE agg_purchases_monthly_user_button PARTITION   (month = '${hiveconf:YYYYMM}')
SELECT 
	currency,
	age,
	gender,
	SUM(revenues) AS revenues, --sum all revenues  that correspond to set of dimensions
	SUM(purchases) AS purchases, --sum  all purchases  that correspond to set of dimensions
	region,
	state,
	count(distinct purch.userid) AS users, --amount of unique users who made purchases
	SUM(purchases)/count(distinct purch.userid) AS average_purchases_per_user,
    '${hiveconf:ENDDATE}' as partition_date
FROM (
	SELECT 
		userid,
		currency,
		SUM(CASE WHEN discountedprice>0 THEN discountedprice ELSE originalprice END) AS revenues, --if no discount then original price is taken, otherwise discount price used for aggr.
		COUNT(userid) AS purchases
	FROM purchase
	WHERE  month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
	AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
    GROUP BY  
    userid,customerid,currency
    )purch
LEFT OUTER JOIN( --joined with location data
SELECT
    userid,
    floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
    gender,
    regionname as region,
    state
    FROM profiling
)profile
ON profile.userid = purch.userid

GROUP BY  
age, gender, currency,region,state;

---
--Create a list of users with first purchase date
---

CREATE EXTERNAL TABLE IF NOT EXISTS first_purchase_user_list( 
userid string,
first_purchase_date string
)
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/first_purchase_user_list';


INSERT INTO TABLE first_purchase_user_list  PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT DISTINCT prch.userid
	,prch.partition_date
FROM purchase prch
WHERE prch.partition_date = '${hiveconf:ENDDATE}'
	AND

NOT EXISTS (
		SELECT userid
		FROM first_purchase_user_list fpul
		WHERE prch.userid = fpul.userid
		);
