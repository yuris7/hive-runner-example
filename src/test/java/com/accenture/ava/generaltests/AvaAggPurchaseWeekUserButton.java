package com.accenture.ava.generaltests;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.config.HiveRunnerConfig;

@RunWith(StandaloneHiveRunner.class)
public class AvaAggPurchaseWeekUserButton {
    @HiveSQL(files = { "sql/profiling.hql", "sql/tv_chanels.hql",
            "sql/purchases/original/agg_purchases_daily_original.hql", "sql/purchases/original/agg_purchases_weekly_user_button.hql" }, autoStart = false)
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
        hiveShell.start();
    }

    @Test
    public void testLoadFilePurchaseWeeklyUserButton() {
        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_weekly_user_button").toArray(new String[0]);
        Assert.assertEquals(12, actual.length);
    }
}
