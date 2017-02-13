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
appversion, paymenttype,currency,
categoryname, genre, channel,
region,state;
