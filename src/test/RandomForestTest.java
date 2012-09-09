package test;
import hexlytics.rf.DRF;
import hexlytics.rf.Tree.StatType;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import water.*;

public class RandomForestTest {

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] {""});
  }

  public RandomForestTest(){
  }

  @org.junit.Test public void testSmallData() {
  System.out.println("Running RandomForest on small data sets");
  String[] datafiles = new String[] { "smalldata/poker/poker100","smalldata/iris/iris2.csv"};
  for (int i=0; i<datafiles.length; i++){
    System.out.println("RandomForest for "+datafiles[i]);
    Key fileKey = TestUtil.load_test_file(new File(datafiles[i]));
    ValueArray va = TestUtil.parse_test_key(fileKey);
    try { Thread.sleep(100); }        // sleep 100msec, to let parse finish & test again
    catch( InterruptedException ie ) {}
    Key entropyR = DRF.web_main(va, 10, 100, .15, StatType.ENTROPY);
    Key    giniR = DRF.web_main(va, 10, 100, .15, StatType.GINI);
    UKV.remove(fileKey); // clean up and burn
    UKV.remove(entropyR);
    UKV.remove(giniR);
	}
  }
	@AfterClass
	public static void shutdown(){
		System.out.println("Shutting down 3 2 1..");
		UDPRebooted.global_kill();
		//System.exit(0);
	}
}
