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
public class AvaReplacedAggAccessesTest {
    @HiveSQL(files = {
            "sql/purchases/original/login.hql",
            "sql/profiling.hql",

            "sql/user_action_original.hql",
            "sql/purchases/original/replaced_agg_accesses.hql"}, autoStart = false)

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
    public void testAccessesTimeband() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_timeband").toArray(new String[0]);
        Assert.assertEquals(53, actual.length);
    }


    @Test
    public void testAggAccessesDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_daily").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }
    @Test
    public void testAggAccessesWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_weekly").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

    @Test
    public void testAggAccessesMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_accesses_monthly").toArray(new String[0]);
        Assert.assertEquals(42, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }
    @Test
    public void testAccessesMultiplePlatformDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_daily").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }

    @Test
    public void testAccessesMultiplePlatformWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_weekly").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }
    @Test
    public void testAccessesMultiplePlatformMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_multiple_platform_monthly").toArray(new String[0]);
        Assert.assertEquals(9, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

    @Test
    public void testAccessesWithWithoutViewDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_with_without_view_daily").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }
    @Test
    public void testAccessesWithWithoutViewTimeband() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_with_without_view_timeband").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }

    @Test
    public void testAccessesWithWithoutViewWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_with_without_view_weekly").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }
    @Test
    public void testAccessesWithWithoutViewMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM accesses_with_without_view_monthly").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

}
