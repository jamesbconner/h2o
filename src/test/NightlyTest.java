package test;

import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import water.UDPRebooted;

/**
 * Created by IntelliJ IDEA.
 * User: sris
 * Date: 9/8/12
 * Time: 1:47 PM
 * To change this template use File | Settings | File Templates.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({RandomForestTest.class})

public class NightlyTest {
	@AfterClass
	public static void shutdown(){
		System.out.println("Shutting down");
		UDPRebooted.global_kill();
		//System.exit(0);
	}
}
