package test;

import static junit.framework.Assert.assertEquals;
import hexlytics.rf.Confusion;
import hexlytics.rf.DRF;
import hexlytics.rf.Tree.StatType;

import org.junit.BeforeClass;
import org.junit.Test;

import water.DKV;
import water.H2O;
import water.Key;
import water.UKV;
import water.ValueArray;
import water.parser.ParseDataset;
import water.util.KeyUtil;

public class RFMarginalCasesTest {
  
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 5000) {
      if (H2O.CLOUD.size() > 2) break;
      try { Thread.sleep(100); } catch( InterruptedException ie ) {}
    }
    assertEquals("Cloud size of 3", 3, H2O.CLOUD.size());    
  }
  
  /* 
   * HTWO-87 bug test
   * 
   *  - two lines dataset (one line is a comment) throws assertion java.lang.AssertionError: classOf no dists > 0? 1 
   */
  @Test public void testTwoLineDataset() {
    Key fkey = KeyUtil.load_test_file("smalldata/test/HTWO-87-two-lines-dataset.csv");
    Key okey = Key.make("HTWO-87-two-lines-dataset.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray val = (ValueArray) DKV.get(okey);
    
    // setup default values for DRF
    int ntrees  = 5;
    int depth   = 30;
    int gini    = StatType.GINI.ordinal();    
    int singlethreaded =  1;
    int seed =  42;
    StatType statType = StatType.values()[gini];

    // Start the distributed Random Forest
    DRF drf = hexlytics.rf.DRF.web_main(val,ntrees,depth,-1.0,statType,seed,singlethreaded==0/*non-blocking*/);
    
    // Create incremental confusion matrix
    Confusion confusion = new Confusion( drf._treeskey, val, ntrees*H2O.CLOUD.size());
    // Just wait
    try { Thread.sleep(2000); } catch( InterruptedException e ) {}
    confusion.refresh();
    
    assertEquals("Number of classes == 1", 1,  confusion._N);
    assertEquals("Confusion matrix [0][0] == 1", 1,  confusion._matrix[0][0]);
        
    UKV.remove(okey);
  }
}
