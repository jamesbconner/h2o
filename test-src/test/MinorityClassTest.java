package test;

import static org.junit.Assert.assertTrue;
import hex.rf.MinorityClasses;
import hex.rf.MinorityClasses.UnbalancedClass;

import java.io.FileInputStream;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import water.*;
import water.parser.ParseDataset;

public class MinorityClassTest {
  static Key _key;
  static ValueArray _data;
  static final int _classIdx = 10;

  static int _nodes = 3;

  @BeforeClass public static void setupCloud() throws Exception {
//    _cloud = H2OTestCloud.startCloud(1, System.out, 20000);
    H2O.main(new String[]{});
    if(_nodes > 1)Thread.sleep(1000);
    FileInputStream f = new FileInputStream("./smalldata/poker/poker-hand-testing.data");
    Key k1 = ValueArray.read_put_file("poker_raw", f,(byte)1);
    _key = Key.make("poker_parsed");
    ParseDataset.parse(_key, DKV.get(k1));
    _data = (ValueArray)DKV.get(_key);
  }

//  @AfterClass public static void destroyCloud() throws IOException {
//    _cloud.cleanup();
//  }
  static int [] expectedHist = new int [] {501209,422498,47622,21121,3885,1996,1424,230,12,3};


  @Test public void testHistogram(){
    int [] h = MinorityClasses.globalHistogram(MinorityClasses.histogram(_data, _classIdx));
    assertTrue(Arrays.equals(expectedHist, h));
  }

  @Test public void testExtraction(){
    UnbalancedClass [] uClasses = MinorityClasses.extractUnbalancedClasses(_data, 10, new int [] {0,9});
    assertTrue(uClasses.length == 2);
    assertTrue(uClasses[0]._chunks.length == 6);
    assertTrue(uClasses[0]._rows == 501209);
    assertTrue(uClasses[1]._chunks.length == 1);
    assertTrue(uClasses[1]._rows == 3);
  }

  public static void main(String [] args){
    if(args.length > 0){
      assert args.length == 1:"unexpected number of args, expects exactl one arg (number of nodes), got " + args.length;
    }
    JUnitCore junit = new JUnitCore();
    Result r = junit.run(MinorityClassTest.class);
    System.out.println("======================================");
    if(r.wasSuccessful()){
      System.out.println("All tests finished successfully!");
    } else {
      System.out.println("Finished with failures:");
      for(Failure f:r.getFailures()){
        System.err.println(f.toString());
        System.err.println(f.getTrace());
      }
    }
    System.exit(0);
  }
}
