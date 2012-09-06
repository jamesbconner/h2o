package test;

import hexlytics.rf.RandomForest;

import java.io.File;
import hexlytics.rf.DRF;
import org.junit.BeforeClass;

import water.*;

public class RFTreeTest {
  @BeforeClass static void setup() {
    H2O.main(new String[] {});    
  }
  
  public static void main(String[] args) throws Exception {
    H2O.main(new String[] {});
    if(args.length==0) args = new String[] { "smalldata/poker/poker-hand-testing.data" };
    Key fileKey = TestUtil.load_test_file(new File(args[0]));
    ValueArray va = TestUtil.parse_test_key(fileKey);
    DKV.remove(fileKey); // clean up and burn
    DRF.web_main(va, 10, 100, .15, false);

  }
}
