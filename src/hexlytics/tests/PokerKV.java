package hexlytics.tests;

import hexlytics.Tree;
import hexlytics.Utils;
import hexlytics.data.Data;
import hexlytics.data.DataAdapter;
import init.init;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetAddress;

import org.junit.BeforeClass;
import org.junit.Test;

import water.DKV;
import water.H2O;
import water.Key;
import water.ValueArray;
import water.csv.CSVParserKV;

public class PokerKV {
  

  static String[] keys = { "poker1000", "poker-hand-testing.data" };

  

  @BeforeClass
  public static void setUpClass() throws Exception {
    InetAddress localhost = InetAddress.getLocalHost();
    String ip = localhost.getHostAddress();
    if(H2O.CLOUD == null)
      init.main(new String [] {"-ip", ip, "-test", "none"});
    Thread.sleep(1000);
  }
  
  @Test
  public void testPokerRF() throws Exception {
    for (String keyStr : keys) {
      System.out.print("parsing key " + keyStr + "...");
      Key k = Key.make(keyStr);
      if (DKV.get(k) == null) {
        System.out.print("key not found, trying to upload the file...");
        File f = new File("smalldata/poker/" + keyStr);
        if (!f.exists()) {
          System.out.println("file not found!");
          continue;
        }
        ValueArray.read_put_file(keyStr, new FileInputStream(f), Key.DEFAULT_DESIRED_REPLICA_FACTOR);
        if(DKV.get(k) == null){
          System.out.println("file upload failed!");
          continue;
        }
        System.out.println("done");        
      }
      DataAdapter poker = new DataAdapter("poker", new String[] { "0", "1", "2",
          "3", "4", "5", "6", "7", "8", "9", "10" }, "10");
      int[] r = new int[11];
      CSVParserKV<int[]> p1 = new CSVParserKV<int[]>(k, Integer.MAX_VALUE, r,
          null);
      double[] v = new double[11];
      for (int[] x : p1) {
        for (int i = 0; i < 11; i++)
          v[i] = x[i];
        poker.addRow(v);
      }
      poker.freeze();
      Data d = Data.make(poker.shrinkWrap());            
      System.out.println(d);
      System.out.println("Computing trees...");
      Data train = d.sampleWithReplacement(.6);
      Data valid = train.complement();
      int[][] score = new int[valid.rows()][valid.classes()];
      for (int i = 0; i < 1000; i++) {
        Tree rf = new Tree();
        rf.compute(train);
        rf.classify(valid, score);
        System.out.println(i + " | err= "
            + Utils.p5d(Tree.score(valid, score)) + " " + rf.tree());
      }
    }
  }
}
