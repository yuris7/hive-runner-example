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
public class AvaAggLoyaltyTest {
    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
        {
            setHiveExecutionEngine("mr");
        }
    };
    @HiveSQL(files = {
            "sql/profiling.hql",
            "sql/purchases/original/watching.hql",
            "sql/purchases/original/agg_loyalty_schemas.hql",
            "sql/profiling.hql",
            "sql/user_action.hql",
            "sql/purchases/original/agg_loyalty.hql"}, autoStart = false)

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
 /**    ASSERTIONS:
                    * loyal
                    * lost
                    * new
                    * reconnected
                    * distinct_users
  */
    @Test
    public void testLoadFileLoyalty() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM watching_customer_base").toArray(new String[0]);
        Assert.assertEquals(43, actual.length);
        		for (String string : actual) {
			System.out.println(">>>>>>>>" + string);
		}
    }

    @Test
    public void testWatchingUsersList() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM watching_users_list").toArray(new String[0]);
        Assert.assertEquals(43, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

    /** Example for LOYAL & LOST
     */

    @Test
    public void testAggLoyaltyDaily() {
        String[] loyal = hiveShell.executeQuery(
                "SELECT loyal FROM agg_loyalty_daily").toArray(new String[0]);
        Assert.assertEquals(10, loyal.length);
        for (String string : loyal) {
            System.out.println(">>>>>>>>" + string);
        }
        String[] lost = hiveShell.executeQuery(
                "SELECT lost FROM agg_loyalty_daily").toArray(new String[0]);
        for (String string : lost) {
            System.out.println(">>>>>>>>" + string);
        }

    }

    @Test
    public void testLoyaltyUserAging() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM loyalty_user_aging").toArray(new String[0]);
        Assert.assertEquals(10, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }
    }

    @Test
    public void testLoyaltyUserAgingWeekly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM loyalty_user_aging_weekly").toArray(new String[0]);
        Assert.assertEquals(10, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }
    @Test
    public void testLoyaltyUserAgingMonthly() {
        String[] actual = hiveShell.executeQuery(
                "SELECT * FROM loyalty_user_aging_monthly").toArray(new String[0]);
        Assert.assertEquals(10, actual.length);
        for (String string : actual) {
            System.out.println(">>>>>>>>" + string);
        }

    }

}
