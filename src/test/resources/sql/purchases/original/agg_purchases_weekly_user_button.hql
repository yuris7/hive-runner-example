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
-- LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/PURCHASE_20160302.csv' OVERWRITE INTO TABLE agg_purchases_weekly_user_button PARTITION (year ='2008', week = 4);

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