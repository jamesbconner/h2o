package test;

import hexlytics.rf.RandomForest;

import java.io.File;

import org.junit.BeforeClass;

import water.*;

public class RFTreeTest {
  @BeforeClass static void setup() {
    H2O.main(new String[] {});    
  }
  
  public static void main(String[] args) throws Exception {
    Key fileKey = TestUtil.load_test_file(new File("smalldata/poker/poker-hand-testing.data"));
    ValueArray va = TestUtil.parse_test_key(fileKey);
    DKV.remove(fileKey); // clean up and burn
//    LocalBuilder builder = RandomForest.web_main(va, 10, 100, .15, true);
//    builder.
  }
}
