package com.accenture.ava;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;

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
public class AvaAggPurchaseTest {
    //,
    //assertThat(actual, hasItemInArray("agg_purchases_daily_user_button"));
    @HiveSQL(files = {"sql/purchases/agg_purchases_plus.hql", "sql/purchases/original/agg_purchases_first_purchase_user_list.hql"}, autoStart = false)
    private HiveShell hiveShell;

    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
        {
            setHiveExecutionEngine("mr");
        }
    };

    @HiveSetupScript
    private String setup = "set hive.support.sql11.reserved.keywords=false; " +
            "SET hive.exec.dynamic.partition = true; SET hive.exec.dynamic.partition.mode = nonstrict; " +
            "SET hive.mapred.mode = nonstrict;";

    @Before
    public void setup() {
        hiveShell.setHiveConfValue("ROOTPATH", "${hiveconf:hadoop.tmp.dir}");
        hiveShell.setHiveConfValue("ENDDATE", "20080815");
        hiveShell.start();
    }
   /*Test for creating Table with Purchases.hql & agg_purchases_first_purchase_user_list.hql
   */
    @Test
    public void testLoadFilePurchasePlus() {
        String[] actual_purchase = hiveShell.executeQuery("SELECT * FROM purchase").toArray(new String[0]);
        String[] actual = hiveShell.executeQuery("SELECT * FROM first_purchase_user_list").toArray(new String[0]);
        Assert.assertEquals(14, actual.length);
    }

//    @Test
//    public void testTablesInListIsCreated() {
//        String[] actual = hiveShell.executeQuery("SHOW TABLES").toArray(new String[0]);
//        assertThat(actual, hasItemInArray("purchase"));
//        assertThat(actual, hasItemInArray("purchase_rejected"));
//        assertThat(actual, hasItemInArray("agg_purchases_daily"));
//
//    }
//
//    @Test
//    public void testPurhasePartition() {
//        String[] actual = hiveShell.executeQuery("SELECT username FROM purchase p WHERE p.partition_date='20080815'")
//                .toArray(new String[0]);
//        assertThat(actual, hasItemInArray("sotnaskrik"));
//        assertThat(actual, hasItemInArray("modosirron"));
//    }
}
