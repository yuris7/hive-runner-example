package com.accenture.ava;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(StandaloneHiveRunner.class)
public class AvaAggAccessesTest {
    @HiveSQL(files = {"sql/purchases/original/login.hql","sql/profiling.hql", "sql/user_action.hql",
            "sql/purchases/original/agg_accesses.hql"}, autoStart = false)

    private HiveShell hiveShell;

    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
        {
            setHiveExecutionEngine("mr");
        }
    };

    @HiveSetupScript
    private String setup = "set hive.support.sql11.reserved.keywords=false; "
            + "SET hive.exec.dynamic.partition = true; SET hive.exec.dynamic.partition.mode = nonstrict; "
            + "SET hive.mapred.mode = nonstrict;";

    @Before
    public void setup() {
        hiveShell.setHiveConfValue("ROOTPATH", "${hiveconf:hadoop.tmp.dir}");
        hiveShell.setHiveConfValue("ENDDATE", "20080815");
        hiveShell.setHiveConfValue("YEAR", "2008");
        hiveShell.setHiveConfValue("WEEK", "4");
        hiveShell.setHiveConfValue("MONTH", "200808");
        hiveShell.start();
    }

    @Test
    public void testLoadFileLoginsTimeBand() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_logins_timeband").toArray(new String[0]);
        Assert.assertEquals(53, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_accesses_is_not_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_timeband WHERE logged_accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_accesses_is_not_null);
        String[] logged_accesses_is_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_timeband WHERE logged_accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_accesses_is_not_null,logged_accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] successful_logins_is_not_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_timeband WHERE successful_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(successful_logins_is_not_null);
        String[] successful_logins_is_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_timeband WHERE successful_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(successful_logins_is_not_null,successful_logins_is_null);
        // assert when --//-- is not NULL and NULL
        String[] failed_logins_is_not_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_timeband WHERE failed_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(failed_logins_is_not_null);
        String[] failed_logins_is_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_timeband WHERE failed_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(failed_logins_is_not_null,failed_logins_is_null);
        // assert when --//-- is not NULL and NULL
        String[] timeband_is_not_null = hiveShell.executeQuery(
                "SELECT timeband FROM agg_logins_timeband WHERE timeband > 0").toArray(new String[0]);
        Assert.assertNotNull(timeband_is_not_null);
        String[] timeband_is_null = hiveShell.executeQuery(
                "SELECT timeband FROM agg_logins_timeband WHERE timeband != 0").toArray(new String[0]);
        Assert.assertNotEquals(timeband_is_not_null, timeband_is_null);
    }

    @Test
    public void testLoadFileLoginsDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_logins_daily").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_accesses_is_not_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_daily WHERE logged_accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_accesses_is_not_null);
        String[] logged_accesses_is_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_daily WHERE logged_accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_accesses_is_not_null,logged_accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] successful_logins_is_not_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_daily WHERE successful_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(successful_logins_is_not_null);
        String[] successful_logins_is_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_daily WHERE successful_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(successful_logins_is_not_null,successful_logins_is_null);
        // assert when --//-- is not NULL and NULL
        String[] failed_logins_is_not_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_daily WHERE failed_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(failed_logins_is_not_null);
        String[] failed_logins_is_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_daily WHERE failed_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(failed_logins_is_not_null,failed_logins_is_null);

    }

    @Test
    public void testLoadFileLoginsWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_logins_weekly").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_accesses_is_not_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_weekly WHERE logged_accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_accesses_is_not_null);
        String[] logged_accesses_is_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_weekly WHERE logged_accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_accesses_is_not_null,logged_accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] successful_logins_is_not_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_weekly WHERE successful_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(successful_logins_is_not_null);
        String[] successful_logins_is_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_weekly WHERE successful_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(successful_logins_is_not_null,successful_logins_is_null);
        // assert when --//-- is not NULL and NULL
        String[] failed_logins_is_not_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_weekly WHERE failed_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(failed_logins_is_not_null);
        String[] failed_logins_is_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_weekly WHERE failed_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(failed_logins_is_not_null,failed_logins_is_null);
    }

    @Test
    public void testLoadFileLoginsMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_logins_monthly").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_accesses_is_not_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_monthly WHERE logged_accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_accesses_is_not_null);
        String[] logged_accesses_is_null = hiveShell.executeQuery(
                "SELECT logged_accesses FROM agg_logins_monthly WHERE logged_accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_accesses_is_not_null,logged_accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] successful_logins_is_not_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_monthly WHERE successful_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(successful_logins_is_not_null);
        String[] successful_logins_is_null = hiveShell.executeQuery(
                "SELECT successful_logins FROM agg_logins_monthly WHERE successful_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(successful_logins_is_not_null,successful_logins_is_null);
        // assert when --//-- is not NULL and NULL
        String[] failed_logins_is_not_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_monthly WHERE failed_logins IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(failed_logins_is_not_null);
        String[] failed_logins_is_null = hiveShell.executeQuery(
                "SELECT failed_logins FROM agg_logins_monthly WHERE failed_logins IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(failed_logins_is_not_null,failed_logins_is_null);

   }

//    accesses_multiple_platform_daily/ accesses_multiple_platform_weekly / accesses_multiple_platform_monthly
//    logged_users_more_platforms int,
//    distinct_users int
    @Test
    public void testLoadFileAccessesMultiplePlatformDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_daily").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_users_more_platforms_is_not_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_daily WHERE logged_users_more_platforms IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_users_more_platforms_is_not_null);
        String[] logged_users_more_platforms_is_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_daily WHERE logged_users_more_platforms IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_users_more_platforms_is_not_null,logged_users_more_platforms_is_null);
        // assert when --//-- is not NULL and NULL
        String[] distinct_users_is_not_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_daily WHERE distinct_users IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(distinct_users_is_not_null);
        String[] distinct_users_is_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_daily WHERE distinct_users IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(distinct_users_is_not_null,distinct_users_is_null);
    }
    @Test
    public void testLoadFileAccessesMultiplePlatformWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_weekly").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_users_more_platforms_is_not_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_weekly WHERE logged_users_more_platforms IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_users_more_platforms_is_not_null);
        String[] logged_users_more_platforms_is_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_weekly WHERE logged_users_more_platforms IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_users_more_platforms_is_not_null,logged_users_more_platforms_is_null);
        // assert when --//-- is not NULL and NULL
        String[] distinct_users_is_not_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_weekly WHERE distinct_users IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(distinct_users_is_not_null);
        String[] distinct_users_is_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_weekly WHERE distinct_users IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(distinct_users_is_not_null,distinct_users_is_null);

    }

    @Test
    public void testLoadFileAccessesMultiplePlatformMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_monthly").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] logged_users_more_platforms_is_not_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_monthly WHERE logged_users_more_platforms IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(logged_users_more_platforms_is_not_null);
        String[] logged_users_more_platforms_is_null = hiveShell.executeQuery(
                "SELECT logged_users_more_platforms FROM accesses_multiple_platform_monthly WHERE logged_users_more_platforms IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(logged_users_more_platforms_is_not_null,logged_users_more_platforms_is_null);
        // assert when --//-- is not NULL and NULL
        String[] distinct_users_is_not_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_monthly WHERE distinct_users IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(distinct_users_is_not_null);
        String[] distinct_users_is_null = hiveShell.executeQuery(
                "SELECT distinct_users FROM accesses_multiple_platform_monthly WHERE distinct_users IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(distinct_users_is_not_null,distinct_users_is_null);

    }

    @Test
    public void testLoadFileLoginDevices() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM login_devices").toArray(new String[0]);
        Assert.assertEquals(57, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] userid_is_not_null = hiveShell.executeQuery(
                "SELECT userid FROM login_devices WHERE userid IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(userid_is_not_null);
        String[] userid_is_null = hiveShell.executeQuery(
                "SELECT userid FROM login_devices WHERE userid IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(userid_is_not_null,userid_is_null);
        // assert when --//-- is not NULL and NULL
        String[] deviceid_is_not_null = hiveShell.executeQuery(
                "SELECT deviceid FROM login_devices WHERE deviceid IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(deviceid_is_not_null);
        String[] deviceid_is_null = hiveShell.executeQuery(
                "SELECT deviceid FROM login_devices WHERE deviceid IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(deviceid_is_not_null,deviceid_is_null);
        // assert when --//-- is not NULL and NULL
        String[] first_access_day_is_not_null = hiveShell.executeQuery(
                "SELECT first_access_day FROM login_devices WHERE first_access_day IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(first_access_day_is_not_null);
        String[] first_access_day_is_null = hiveShell.executeQuery(
                "SELECT first_access_day FROM login_devices WHERE first_access_day IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(first_access_day_is_not_null,first_access_day_is_null);
        // assert when --//-- is not NULL and NULL
        String[] last_access_day_is_not_null = hiveShell.executeQuery(
                "SELECT last_access_day FROM login_devices WHERE last_access_day IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(first_access_day_is_not_null);
        String[] last_access_day_is_null = hiveShell.executeQuery(
                "SELECT last_access_day FROM login_devices WHERE last_access_day IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(last_access_day_is_not_null,last_access_day_is_null);

    }

    @Test
    public void testLoadFileAnonymousDevices() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM anonymous_devices").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] userid_is_not_null = hiveShell.executeQuery(
                "SELECT userid FROM anonymous_devices WHERE userid IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(userid_is_not_null);
        String[] userid_is_null = hiveShell.executeQuery(
                "SELECT userid FROM anonymous_devices WHERE userid IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(userid_is_not_null,userid_is_null);
        // assert when --//-- is not NULL and NULL
        String[] deviceid_is_not_null = hiveShell.executeQuery(
                "SELECT deviceid FROM anonymous_devices WHERE deviceid IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(deviceid_is_not_null);
        String[] deviceid_is_null = hiveShell.executeQuery(
                "SELECT deviceid FROM anonymous_devices WHERE deviceid IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(deviceid_is_not_null,deviceid_is_null);
        // assert when --//-- is not NULL and NULL
        String[] first_access_day_is_not_null = hiveShell.executeQuery(
                "SELECT first_access_day FROM anonymous_devices WHERE first_access_day IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(first_access_day_is_not_null);
        String[] first_access_day_is_null = hiveShell.executeQuery(
                "SELECT first_access_day FROM anonymous_devices WHERE first_access_day IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(first_access_day_is_not_null,first_access_day_is_null);
        // assert when --//-- is not NULL and NULL
        String[] last_access_day_is_not_null = hiveShell.executeQuery(
                "SELECT last_access_day FROM anonymous_devices WHERE last_access_day IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(first_access_day_is_not_null);
        String[] last_access_day_is_null = hiveShell.executeQuery(
                "SELECT last_access_day FROM anonymous_devices WHERE last_access_day IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(last_access_day_is_not_null,last_access_day_is_null);

    }

    @Test
    public void testLoadFileAggAccessesDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_daily").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] accesses_is_not_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_daily WHERE accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_is_not_null);
        String[] accesses_is_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_daily WHERE accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_is_not_null,accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_with_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_daily WHERE accesses_with_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_with_views_is_not_null);
        String[] accesses_with_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_daily WHERE accesses_with_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_with_views_is_not_null,accesses_with_views_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_without_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_daily WHERE accesses_without_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_without_views_is_not_null);
        String[] accesses_without_views_is_null = hiveShell.executeQuery(
                "SELECT first_access_day FROM agg_accesses_daily WHERE first_access_day IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_without_views_is_not_null,accesses_without_views_is_null);

    }

    @Test
    public void testLoadFileagg_accesses_timeband() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_timeband").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] timeband_is_not_null = hiveShell.executeQuery(
                "SELECT timeband FROM agg_accesses_timeband WHERE timeband IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(timeband_is_not_null);
        String[] timeband_is_null = hiveShell.executeQuery(
                "SELECT timeband FROM agg_accesses_timeband WHERE timeband IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(timeband_is_not_null,timeband_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_is_not_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_timeband WHERE accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_is_not_null);
        String[] accesses_is_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_timeband WHERE accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_is_not_null,accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_with_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM accesses_with_views WHERE accesses_with_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_with_views_is_not_null);
        String[] accesses_with_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_timeband WHERE accesses_with_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_with_views_is_not_null,accesses_with_views_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_without_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_timeband WHERE accesses_without_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_with_views_is_not_null);
        String[] accesses_without_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_timeband WHERE accesses_without_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_without_views_is_not_null,accesses_without_views_is_null);

    }

    @Test
    public void testLoadFileAggAccessesWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_weekly").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] accesses_is_not_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_weekly WHERE accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_is_not_null);
        String[] accesses_is_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_weekly WHERE accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_is_not_null,accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_with_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_weekly WHERE accesses_with_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_with_views_is_not_null);
        String[] accesses_with_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_weekly WHERE accesses_with_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_with_views_is_not_null,accesses_with_views_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_without_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_weekly WHERE accesses_without_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_without_views_is_not_null);
        String[] accesses_without_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_weekly WHERE accesses_without_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_without_views_is_not_null,accesses_without_views_is_null);

    }

    @Test
    public void testLoadFileAggAccessesMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_monthly").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        // assert when --//-- is not NULL and NULL
        String[] accesses_is_not_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_monthly WHERE accesses IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_is_not_null);
        String[] accesses_is_null = hiveShell.executeQuery(
                "SELECT accesses FROM agg_accesses_monthly WHERE accesses IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_is_not_null,accesses_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_with_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_monthly WHERE accesses_with_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_with_views_is_not_null);
        String[] accesses_with_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_with_views FROM agg_accesses_monthly WHERE accesses_with_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_with_views_is_not_null,accesses_with_views_is_null);
        // assert when --//-- is not NULL and NULL
        String[] accesses_without_views_is_not_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_monthly WHERE accesses_without_views IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(accesses_without_views_is_not_null);
        String[] accesses_without_views_is_null = hiveShell.executeQuery(
                "SELECT accesses_without_views FROM agg_accesses_monthly WHERE accesses_without_views IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(accesses_without_views_is_not_null,accesses_without_views_is_null);
    }
}
