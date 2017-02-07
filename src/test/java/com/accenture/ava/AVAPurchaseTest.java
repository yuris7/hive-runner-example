package com.accenture.ava;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;

@RunWith(StandaloneHiveRunner.class)
public class AVAPurchaseTest {

	@HiveSQL(files = { "sql/purchases/purchases.hql", "sql/purchases/agg_purchases_daily.hql" }, autoStart = false)
	private HiveShell hiveShell;

	@HiveSetupScript
	private String setup = "set hive.support.sql11.reserved.keywords=false; SET hive.exec.dynamic.partition = true; SET hive.exec.dynamic.partition.mode = nonstrict; SET hive.mapred.mode = nonstrict;";

	@Before
	public void setup() {
		hiveShell.setHiveConfValue("ROOTPATH", "${hiveconf:hadoop.tmp.dir}");
		// hiveShell.addSetupScript("CREATE table FOO (s String) LOCATION
		// '${hiveconf:location}'");
		hiveShell.setHiveConfValue("date", "spanish");
		hiveShell.start();
		// hiveShell.insertInto("test_db", "test_table").addRow("v1").commit();
	}

	// SET hive.exec.dynamic.partition = true;
	// SET hive.exec.dynamic.partition.mode = nonstrict;
	// SET hive.mapred.mode = nonstrict;
	@Test
	public void testLoadFilePurchase() {
		// hiveShell.executeQuery("INSERT INTO TABLE purchase PARTITION
		// (partition_date = '" + endDate + "'");
		String[] actual = hiveShell.executeQuery("SELECT * FROM purchase").toArray(new String[0]);
		// for (String string : actual) {
		// System.out.println(">>>>>>>>" + string);
		// }
		// Assert.assertEquals(644, actual.length);
	}

	@Test
	public void testShowTables() {
		String[] actual = hiveShell.executeQuery("SHOW TABLES").toArray(new String[0]);
		assertThat(actual, hasItemInArray("purchase"));
	}
}