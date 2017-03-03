-- creation of target table for join entity  -- TABLE JOIN WAS CHANGED TO  SJOIN IN sjoin.hql & agg_registrations.hql FILES
CREATE EXTERNAL TABLE IF NOT EXISTS sjoin (
  tenantid STRING,
  date STRING,
  time STRING,
  loginemail STRING,
  clientipaddress STRING,
  username STRING,
  uniquecontract STRING,
  timestamp STRING,
  servicename STRING,
  providername STRING,
  referralsource STRING,
  registrationmethod STRING,
  registrationtrigger STRING
)
--- PARTITIONED BY (partition_date STRING)
--- ROW FORMAT DELIMITED
--- FIELDS TERMINATED BY '\073' ESCAPED BY '\\'
--- LINES TERMINATED BY '\n'
--- STORED AS TEXTFILE
--- LOCATION '${hiveconf:ROOTPATH}/AVA/JOIN/input'
PARTITIONED BY (partition_date STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INPATH 'src/test/resources/sql/purchases/original/JOIN_20160302.CSV' OVERWRITE INTO TABLE sjoin PARTITION (partition_date='20080815');
---
--table structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS login_users_list(
user_id string,
access_day string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/login_users_list';

---
-- List of all unique registered users and their first day of successful access. 
---

INSERT INTO TABLE login_users_list
SELECT  
	new.* 
	FROM (
	SELECT userid, 
	'${hiveconf:ENDDATE}' AS access_day
	FROM login
	WHERE partition_date = '${hiveconf:ENDDATE}' 
	AND eventtype  = 'LOGIN'
	AND loginsuccess = 'Y'
    GROUP BY userid
)new
LEFT OUTER JOIN login_users_list existed
ON (existed.user_id = new.userid) 
WHERE existed.user_id is null;

---
-- table structure
---

CREATE EXTERNAL TABLE IF NOT EXISTS access_customer_base(
user_id string,
user_type string)
partitioned by (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/access_customer_base';

---
-- Daily Unique User Accesses-to reduce processing time and complexity of VAR aggr.
---
INSERT INTO TABLE access_customer_base PARTITION (partition_date = '${hiveconf:ENDDATE}')
SELECT 
userid,
IF(customerid = 'GUEST', 'GUEST', 'REGISTERED') AS user_type
FROM user_action
WHERE  partition_date = '${hiveconf:ENDDATE}'
AND eventtype = 'LOADHOME'
AND terminationreason = ''
GROUP BY userid, customerid;

---
--table structure
---
CREATE EXTERNAL TABLE IF NOT EXISTS access_users_list(
user_id string,
user_type string,
access_day string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/access_users_list';

---
--List of unique users first access day
---

INSERT INTO TABLE access_users_list
SELECT  
	new.* 
	FROM (
	SELECT user_id, 
	user_type, 
	partition_date AS access_day
	FROM access_customer_base
	WHERE partition_date = '${hiveconf:ENDDATE}' 
)new
LEFT OUTER JOIN access_users_list existed
ON (existed.user_id = new.user_id) 
WHERE existed.user_id is null;

--
--
---

CREATE TABLE IF NOT EXISTS join_users_list(
uniquecontract string,
date string,
time string,
referralsource string,
registrationmethod	string,
registrationtrigger	string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/join_users_list';


INSERT OVERWRITE TABLE join_users_list
SELECT
	COALESCE(new_reg.uniquecontract, all_reg.uniquecontract) as uniquecontract,
	COALESCE(new_reg.date, all_reg.date) as date,
	COALESCE(new_reg.time, all_reg.time) as time,
	COALESCE(new_reg.referralsource, all_reg.referralsource) as referralsource,
	COALESCE(new_reg.registrationmethod, all_reg.registrationmethod) as registrationmethod,
	COALESCE(new_reg.registrationtrigger, all_reg.registrationtrigger) as registrationtrigger
FROM (
	SELECT
		uniquecontract,
		date,
		time,
		referralsource,
		registrationmethod,
		registrationtrigger	
		from sjoin
	WHERE partition_date = '${hiveconf:ENDDATE}') new_reg
	FULL OUTER JOIN join_users_list all_reg
	ON all_reg.uniquecontract  = new_reg.uniquecontract
