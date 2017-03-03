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
public class AvaVarAnonymousDaily {
    @HiveSQL(files = {
            "sql/purchases/original/login.hql",
            "sql/vod_catalog.hql",
            "sql/profiling.hql",
            "sql/tv_chanels.hql",
            "sql/tvchanels_rejected.hql",
            "sql/tvchannels_staging.hql",
            "sql/user_action.hql",
            "sql/purchases/original/var_related.hql",
            "sql/purchases/original/sjoin.hql",
            "sql/purchases/original/agg_registrations.hql",
            "sql/purchases/original/agg_loyalty_schemas.hql",
            "sql/purchases/original/watching.hql",
            "sql/purchases/original/var_anonymous_daily.hql",
            "sql/vod_catalog.hql"}, autoStart = false)

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
    public void testVarAnonymousBase() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM var_anonymous_base").toArray(new String[0]);
        Assert.assertEquals(0, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }
}
