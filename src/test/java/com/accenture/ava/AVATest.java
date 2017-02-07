package com.accenture.ava;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.List;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;

@RunWith(StandaloneHiveRunner.class)
public class AVATest {

	@HiveSQL(files = { "sql/user_action.hql",
			"sql/user_action_rejected.hql",
			"sql/vod_catalog.hql",
			"sql/vod_catalog_rejected.hql",
			"sql/vod_catalog_staging.hql"}, autoStart = false)
	private HiveShell hiveShell;

	@HiveSetupScript
	private String setup = "set hive.support.sql11.reserved.keywords=false;";

	@Before
	public void setup() {

		hiveShell.start();
	}
	@Test
	public void testHiveUserActionTableSQLLoaded() {
		List<String> actual = hiveShell.executeQuery("show tables");
		String[] actualArray = actual.toArray(new String[0]);
		assertThat(actualArray, hasItemInArray("user_action"));
	}

	@Test
	public void testHiveUserActionRejectedTableSQLLoaded() {
		List<String> actual = hiveShell.executeQuery("show tables");
		String[] actualArray = actual.toArray(new String[0]);
		assertThat(actualArray, hasItemInArray("user_action_rejected"));
	}

	@Test
	public void testHiveVodCatalogStagingTableSQLLoaded() {
		List<String> actual = hiveShell.executeQuery("show tables");
		String[] actualArray = actual.toArray(new String[0]);
		assertThat(actualArray, hasItemInArray("vod_catalog_staging"));
	}

	@Test
	public void testHiveVodCatalogRejectedTableSQLLoaded() {
		List<String> actual = hiveShell.executeQuery("show tables");
		String[] actualArray = actual.toArray(new String[0]);
		assertThat(actualArray, hasItemInArray("vod_catalog_rejected"));
	}

	@Test
	public void testHiveVodCatalogTableSQLLoaded() {
		List<String> actual = hiveShell.executeQuery("show tables");
		String[] actualArray = actual.toArray(new String[0]);
		assertThat(actualArray, hasItemInArray("vod_catalog"));
	}

}