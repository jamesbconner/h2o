package water.test;

import java.io.File;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import test.analytics.AlphaDSetAdapter;
import water.H2O;
import analytics.RF;

public class LargeDataRFTest {
  String filename = "E:\\datasets\\poker-hand-testing.data";

  @Test
  public void testRF(){
    File dataF = new File("E:\\datasets\\alpha_train.dat");
    File labelsF = new File("E:\\datasets\\alpha_train.lab");
    Assert.assertTrue(dataF.exists());
    Assert.assertTrue(labelsF.exists());
    AlphaDSetAdapter adapter = new AlphaDSetAdapter(dataF, labelsF);
    RF rf = new RF(adapter,100);
    rf.compute();
    System.out.println(H2O.SELF + " done: " + rf);
  }
}


