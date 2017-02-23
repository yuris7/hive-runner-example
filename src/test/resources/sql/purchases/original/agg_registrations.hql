---
-- table structure for daily KPIs
---
CREATE EXTERNAL TABLE IF NOT EXISTS agg_registrations_daily(
registrationmethod string,
registrationtrigger string,
referralsource string,
age tinyint,
gender char(1),
state string,
region string,
registrations int
    )
PARTITIONED BY (PARTITION_DATE STRING)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_registrations_daily';

---
-- Registration related KPIs
-- A_66,A_67,A_68,A_70,A_71,A_72
---
INSERT INTO TABLE agg_registrations_daily PARTITION  (partition_date = '${hiveconf:ENDDATE}') 
SELECT 
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25) as age,
	gender,
	state,
	regionname as region,
	count(uniquecontract) as Registrations
FROM sjoin join_
LEFT OUTER JOIN PROFILING loc --merge sjoin and profiling data
ON loc.userid = join_.uniquecontract -- by key = userid
WHERE join_.partition_date = '${hiveconf:ENDDATE}' --specify day
GROUP BY --group by required dimensions
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	floor(datediff(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')),from_unixtime(unix_timestamp(birth_date, 'yyyyMMdd')))/365.25),
	gender,
	state,
	regionname; 
	
---
-- structure for weekly registration related KPIs
---

CREATE TABLE IF NOT EXISTS agg_registrations_weekly(
registrationmethod string,
registrationtrigger string,
referralsource string,
age tinyint,
gender char(1),
state string,
region string,
registrations int,
partition_date string
    )
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_registrations_weekly';
---
-- weekly aggregations
---

INSERT OVERWRITE TABLE agg_registrations_weekly PARTITION  (year = '${hiveconf:YEAR}', week)
SELECT 
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	age,
	gender,
	state,
	region,
	sum(Registrations) as registrations,
	'${hiveconf:ENDDATE}' as partition_date,
	weekofyear(from_unixtime(unix_timestamp( '${hiveconf:ENDDATE}', 'yyyyMMdd'))) as week
FROM agg_registrations_daily
WHERE weekofyear(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = weekofyear(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 	
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	age,
	gender,
	state,
	region;

---
-- structure for monthly KPIs
---

CREATE TABLE IF NOT EXISTS agg_registrations_monthly(
registrationmethod string,
registrationtrigger string,
referralsource string,
age tinyint,
gender char(1),
state string,
region string,
registrations int,
partition_date string)
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_registrations_monthly';

---
-- monthly aggregates
---

INSERT OVERWRITE TABLE agg_registrations_monthly PARTITION   (month = '${hiveconf:YYYYMM}')
SELECT 
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	age,
	gender,
	state,
	region,
	sum(Registrations) as registrations,
	'${hiveconf:ENDDATE}' as partition_date
FROM agg_registrations_daily
WHERE  month(from_unixtime(unix_timestamp(partition_date, 'yyyyMMdd'))) = month(from_unixtime(unix_timestamp('${hiveconf:ENDDATE}', 'yyyyMMdd')))
AND  '${hiveconf:YEAR}' = substr(partition_date,0,4)
GROUP BY 	
	RegistrationMethod,	
	RegistrationTrigger,	
	ReferralSource,
	age,
	gender,
	state,
	region;
