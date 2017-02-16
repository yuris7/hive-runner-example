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
public class AvaAggPurchasesDaily {
	@HiveSQL(files = { "sql/profiling.hql", "sql/tv_chanels.hql",
			"sql/purchases/original/agg_purchases_daily_original.hql" }, autoStart = false)
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
		// hiveShell.setHiveConfValue("YEAR", "2008");
		hiveShell.start();
	}

	@Test
	public void testLoadFilePurchaseDaily() {
		String[] actual = hiveShell.executeQuery("SELECT * FROM agg_purchases_daily").toArray(new String[0]);
		Assert.assertEquals(53, actual.length);
		String[] age_is_not_null = hiveShell.executeQuery("SELECT age FROM agg_purchases_daily WHERE age IS NOT NULL").toArray(new String[0]);
		Assert.assertNotNull(age_is_not_null);
//		for (String string : age_is_not_null) {
//			System.out.println(">>>>>>>>" + string);
//		}
		String[] age_is_null = hiveShell.executeQuery("SELECT age FROM agg_purchases_daily WHERE age IS NULL").toArray(new String[0]);
		Assert.assertNotEquals(age_is_not_null,age_is_null);
	}

}