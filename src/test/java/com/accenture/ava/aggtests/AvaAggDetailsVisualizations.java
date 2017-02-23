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
public class AvaAggDetailsVisualizations {
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
        {
            setHiveExecutionEngine("mr");
        }
    };
    @HiveSQL(files = {
            "sql/vod_catalog.hql",
            "sql/tv_chanels.hql",
            "sql/profiling.hql",
            "sql/purchases/original/watching.hql",
            "sql/purchases/original/agg_loyalty_schemas.hql",
            "sql/profiling.hql",
            "sql/user_action.hql",
            "sql/purchases/original/agg_details_visualizations.hql"}, autoStart = false)

    private HiveShell hiveShell;
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
    public void testAggDetailsVisualDaily() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_details_visual_daily").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }
    @Test
    public void testAggDetailsVisualWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_details_visual_weekly").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

    @Test
    public void testAggDetailsVisualMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_details_visual_monthly").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }
    @Test
    public void testAggDetailsVisualTimeband() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM agg_details_visual_timeband").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }
}
