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

public class TestPokerDRF {
  

  //static String[] keys = { "poker1000", "poker-hand-testing.data" };

  static String[] keys = { "poker-hand-testing.data" };
  

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
      PokerDRF pkr = new PokerDRF(k,5,keyStr);
      pkr.doRun();
    }
  }
}
