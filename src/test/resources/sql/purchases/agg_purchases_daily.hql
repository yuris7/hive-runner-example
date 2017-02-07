---
-- purchases 1D KPIs schema
---
CREATE EXTERNAL TABLE IF NOT EXISTS agg_purchases_daily(platform string, age tinyint, gender char(1), appversion string, paymenttype string, currency string, revenues decimal(15,2), purchases int, categoryname string, contenttype string, genre string, channel string, region string, state string)