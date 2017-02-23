package com.accenture.ava.aggtests;

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
public class AvaAggAccessesTimePeak {
    @HiveSQL(files = {
            "sql/user_action.hql",
            "sql/purchases/original/login.hql",
            "sql/profiling.hql",
            "sql/purchases/original/agg_accesses.hql",
            "sql/purchases/original/login.hql",
            "sql/profiling.hql", "sql/user_action.hql",
            "sql/purchases/original/agg_accesses_time_peak.hql"}, autoStart = false)

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
    public void testLoadFileAccessesMaximumTimePeakDaily() {
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
    public void testLoadFileAccessesMaximumTimePeakWeekly() {
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
    public void testLoadFileAccessesMaximumTimePeakMonthly() {
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
}
