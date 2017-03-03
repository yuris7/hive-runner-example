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
public class AvaVarLoggedTest {
        @HiveSQL(files = {
                "sql/purchases/original/login.hql",
                "sql/vod_catalog.hql",
                "sql/profiling.hql",
                "sql/tv_chanels.hql",
                "sql/user_action1.hql",
                "sql/purchases/original/var_related.hql",
                "sql/purchases/original/sjoin.hql",
                "sql/purchases/original/agg_registrations.hql",
                "sql/purchases/original/agg_loyalty_schemas.hql",
                "sql/purchases/purchases.hql",
                "sql/purchases/original/watching.hql",
                "sql/purchases/original/var_logged_daily.hql",
                "sql/purchases/original/var_logged.hql"
               }, autoStart = false)

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
            hiveShell.setHiveConfValue("FRAME", "7306090");

            hiveShell.start();
        }

        @Test
        public void testVarLogged() {
            String[] actual = hiveShell.executeQuery(
                    "SELECT * FROM var_logged_${hiveconf:FRAME}D").toArray(new String[0]);
            Assert.assertEquals(42, actual.length);
            for (String string : actual) {
                System.out.println(">>>>>>>>" + string);
            }
        }
}
