package com.accenture.ava.others;

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
public class AVAVodCatalogTest {

    @HiveSQL(files = {"sql/vod_catalog.hql"}, autoStart = false)
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
        String[] actual = hiveShell.executeQuery("SELECT * FROM vod_catalog").toArray(new String[0]);
        Assert.assertEquals(236, actual.length);
    }

}
