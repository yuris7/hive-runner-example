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
public class AVAPurchaseTest {

	@HiveSQL(files = { "sql/purchases/purchases.hql", "sql/purchases/purchases-rejected.hql",
			"sql/purchases/agg_purchases_daily.hql" }, autoStart = false)
	private HiveShell hiveShell;

	@HiveRunnerSetup
	public final HiveRunnerConfig CONFIG = new HiveRunnerConfig() {
		{
			setHiveExecutionEngine("mr");
		}
	};

	@HiveSetupScript
	private String setup = "set hive.support.sql11.reserved.keywords=false; SET hive.exec.dynamic.partition = true; SET hive.exec.dynamic.partition.mode = nonstrict; SET hive.mapred.mode = nonstrict;";

	@Before
	public void setup() {
		hiveShell.setHiveConfValue("ROOTPATH", "${hiveconf:hadoop.tmp.dir}");
		hiveShell.setHiveConfValue("ENDDATE", "2008-08-15");
		hiveShell.start();
		// hiveShell.insertInto("test_db", "test_table").addRow("v1").commit();
	}

	@Test
	public void testLoadFilePurchase() {
		String[] actual = hiveShell.executeQuery("SELECT * FROM purchase").toArray(new String[0]);
		Assert.assertEquals(36, actual.length);
	}

	@Test
	public void testTablesInListIsCreated() {
		String[] actual = hiveShell.executeQuery("SHOW TABLES").toArray(new String[0]);
		assertThat(actual, hasItemInArray("purchase"));
		assertThat(actual, hasItemInArray("purchase_rejected"));
	}

	@Test
	public void testPurhasePartition() {
		String[] actual = hiveShell.executeQuery("SELECT username FROM purchase p WHERE p.partition_date='2008-08-15'")
				.toArray(new String[0]);
		assertThat(actual, hasItemInArray("sotnaskrik"));
		assertThat(actual, hasItemInArray("modosirron"));
	}
}