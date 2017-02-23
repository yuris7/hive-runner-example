---
-- Dily Active Users/ neccessary to reduce processing time and complexity of Loyalty related aggr.
---

create external table if not exists watching_customer_base(
user_id string,
customerid string)
partitioned by (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/watching_customer_base';


---
--List of all users + first day of action/on platform
---

create external table if not exists watching_users_list(
user_id string,
customer_id string,
start_day string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/watching_users_list';

---
-- Loyal/Lost/Reconnected users by dimensions
---

create external table if not exists agg_loyalty_daily(
user_type string,
state string, 
region string,
loyal int,
lost int,
new int,
reconnected int)
partitioned by (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_loyalty_daily';

---
-- schema for User Aging KPIs
---

create external table if not exists loyalty_user_aging (
user_aging string,
user_type string,
region string,
state string,
distinct_users int
)
partitioned by (partition_date string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/loyalty_user_aging';

---
-- schema for User Aging KPIs 7D
---

CREATE TABLE IF NOT EXISTS loyalty_user_aging_weekly (
user_aging string,
user_type string,
region string,
state string,
distinct_users int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/loyalty_user_aging_weekly';

---
-- schema for User Aging KPIs month
---

CREATE TABLE IF NOT EXISTS loyalty_user_aging_monthly (
user_aging string,
user_type string,
region string,
state string,
distinct_users int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/loyalty_user_aging_monthly';

---
-- table structure  A_316, A_317, A_318,A_319
---
CREATE TABLE IF NOT EXISTS agg_loyalty_weekly(
user_type string,
state string,
region string,
loyal int,
lost int,
new int,
reconnected int,
partition_date string)
PARTITIONED BY (year string, week int)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_loyalty_weekly';

---
-- table structure  A_316, A_317, A_318,A_319
---
CREATE TABLE IF NOT EXISTS agg_loyalty_monthly(
user_type string,
state string,
region string,
loyal int,
lost int,
new int,
reconnected int,
partition_date string
    )
PARTITIONED BY (month string)
LOCATION '${hiveconf:ROOTPATH}/processed/AVA/agg_loyalty_monthly';

