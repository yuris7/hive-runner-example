package com.accenture.ava;

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
public class AVAGeneralAgregateTest {
    @HiveSQL(files = {"sql/vod_catalog.hql",
            "sql/tv_chanels.hql",
            "sql/profiling.hql",
            "sql/purchases/purchases.hql",
            "sql/agg_purchases_original.hql",
            "sql/purchases/original/agg_purchases_monthly.hql",
            "sql/purchases/agg_purchases_plus_first_purchase_user_list.hql",
            "sql/purchases/original/purchase_user_list.hql"}, autoStart = false)

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
    public void testLoadFilePurchaseDaily() {
        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_daily").toArray(new String[0]);
        Assert.assertEquals(17, actual.length);
        String[] age = hiveShell.executeQuery("SELECT age FROM agg_purchases_daily").toArray(new String[0]);
//        System.out.println(age);
        String[] age_is_not_null = hiveShell.executeQuery("SELECT age FROM agg_purchases_daily WHERE age IS NOT NULL").toArray(new String[0]);
        Assert.assertNotNull(age_is_not_null);
//		for (String string : age_is_not_null) {
//			System.out.println(">>>>>>>>" + string);
//		}
        String[] age_is_null = hiveShell.executeQuery("SELECT age FROM agg_purchases_daily WHERE age IS NULL").toArray(new String[0]);
        Assert.assertNotEquals(age_is_not_null,age_is_null);

    }
    /**
     @Test
     public void testPurhasePartition() {
     String[] actual = hiveShell.executeQuery("SELECT username FROM purchase p WHERE p.partition_date='20080815'")
     .toArray(new String[0]);
     assertThat(actual, hasItemInArray("sotnaskrik"));
     assertThat(actual, hasItemInArray("modosirron"));
     }

     @Test
     public void testPurhaseUserId() {
     String[] actual1 = hiveShell.executeQuery("SELECT userid FROM first_purchase_user_list f WHERE f.partition_date='20080815'")
     .toArray(new String[0]);
     assertThat(actual1, hasItemInArray("300000001"));

     }


     */

//    @Test
//    public void testLoadFilePurchaseWeekly() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_weekly").toArray(new String[0]);
//        Assert.assertEquals(17, actual.length);
//    }
//
//    @Test
//    public void testLoadFilePurchaseMonth() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_monthly").toArray(new String[0]);
//        Assert.assertEquals(17, actual.length);
//    }
//
//    @Test
//    public void testLoadFilePurchaseUserList() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM purchase_user_list").toArray(new String[0]);
//        Assert.assertEquals(18, actual.length);
//    }
//
//    @Test
//    public void testLoadFileFirstPurchaseUserList() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM first_purchase_user_list").toArray(new String[0]);
//        Assert.assertEquals(14, actual.length);
//    }
//
//    @Test
//    public void testLoadFilePurchaseDailyUserButton() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_daily_user_button").toArray(new String[0]);
//        Assert.assertEquals(12, actual.length);
//    }
//
//    @Test
//    public void testLoadFilePurchaseWeeklyUserButton() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_weekly_user_button").toArray(new String[0]);
//        Assert.assertEquals(12, actual.length);
//    }
//
//    @Test
//    public void testLoadFilePurchaseMonthUserButton() {
//        String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_monthly_user_button").toArray(new String[0]);
//        Assert.assertEquals(12, actual.length);
//    }

}
