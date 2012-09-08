package test;
import hexlytics.rf.DRF;
import hexlytics.rf.Tree;
import org.junit.BeforeClass;
import water.H2O;
import water.Key;
import water.UKV;
import water.ValueArray;

import java.io.File;

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

           Key entropyR = DRF.web_main(va, 10, 100, .15, Tree.StatType.oldEntropy);
           Key    giniR = DRF.web_main(va, 10, 100, .15, Tree.StatType.gini);

			UKV.remove(fileKey); // clean up and burn
			UKV.remove(entropyR);
			UKV.remove(giniR);
    }
  }
}
