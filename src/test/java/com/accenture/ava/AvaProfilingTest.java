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
public class AvaProfilingTest {

    @HiveSQL(files = {"sql/profiling.hql"}, autoStart = false)
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
        // hiveShell.insertInto("test_db", "test_table").addRow("v1").commit();
    }

    @Test
    public void testLoadFileProfiling() {
        String[] actual = hiveShell.executeQuery("SELECT * FROM profiling").toArray(new String[0]);
        Assert.assertEquals(219, actual.length);
    }

//    @Test
//    public void testTablesInListIsCreated() {
//        String[] actual = hiveShell.executeQuery("SHOW TABLES").toArray(new String[0]);
//        assertThat(actual, hasItemInArray("profiling"));
//        assertThat(actual, hasItemInArray("profiling_new"));
//        assertThat(actual, hasItemInArray("profiling_old"));
//        assertThat(actual, hasItemInArray("profiling_rejected"));
//        assertThat(actual, hasItemInArray("profiling_staging"));
//        assertThat(actual, hasItemInArray("profiling_upd"));
//
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
