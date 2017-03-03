package com.accenture.ava.generaltests;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsArrayContaining.hasItemInArray;

@RunWith(StandaloneHiveRunner.class)
public class AVACSVTest {
	@HiveSQL(files = {"sql/user_action.hql"}, autoStart = false)
	private HiveShell hiveShell;

	@HiveSetupScript
	private String setup = "set hive.support.sql11.reserved.keywords=false;";

	@Before
	public void setup() {

		hiveShell.start();
	}

	@Test
	public void testLoadFileResources() {
		String[] actual = hiveShell.executeQuery("SELECT * FROM user_action").toArray(new String[0]);
//		 for (String string : actual) {
//		 System.out.println(">>>>>>>>" + string);
//		 }
		Assert.assertEquals(644, actual.length);
	}

	@Test
	public void testTableContainName() {
		String[] actual = hiveShell.executeQuery("SELECT username FROM user_action WHERE username == 'sleinadlabina'")
				.toArray(new String[0]);
		Assert.assertEquals(3, actual.length);
	}

	@Test
	public void testTableContainsNames() {
		String[] actual = hiveShell.executeQuery("SELECT username FROM user_action").toArray(new String[0]);
		assertThat(actual, hasItemInArray("sleinadlabina"));
		assertThat(actual, hasItemInArray("sotnaskrik"));
		assertThat(actual, hasItemInArray("hplodnarnnod"));
	}
}
