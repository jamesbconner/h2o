package water.test;

import init.init;

import org.junit.BeforeClass;
import org.junit.Test;

import sun.reflect.ReflectionFactory.GetReflectionFactoryAction;
import test.analytics.DRFBuilder;
import water.DKV;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.UDP;
import water.Value;

public class DRFTest {
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    if(H2O.CLOUD == null)
      init.main(new String [] {"-ip", "192.168.56.1", "-test", "none"});
  }
  
  @Test
  public void testPokerDataSet(){
    String filename = "C:\\datasets\\poker-hand-testing.data";
    byte [] filenameBytes = filename.getBytes();
    Key [] keys = new Key[3];
    int kIdx = 0;
    int offset = 0;
    int chunkLen = 10*1024*1024;
    for(H2ONode node:H2O.CLOUD._memary){
      if(kIdx == 3)break;
      keys[kIdx] = Key.make("RFTest" + kIdx,(byte)1,Key.DFJ_INTERNAL_USER, H2O.CLOUD._memary[kIdx]);      
      Value v = new Value(keys[kIdx],1024);
      byte [] mem = v.mem();
      UDP.set4(mem, 0, filenameBytes.length);
      System.arraycopy(filenameBytes, 0, mem, 4, filenameBytes.length);
      UDP.set4(mem, 4 + filenameBytes.length, offset);
      UDP.set4(mem, 4 + filenameBytes.length + 4, chunkLen);
      offset += chunkLen;
      UDP.set4(mem, 4 + filenameBytes.length + 4 + 4, 1);
      DKV.put(keys[kIdx], v);
      ++kIdx;
    }     
    DRFBuilder builder = new DRFBuilder();
    for(Key k:keys)System.out.println(k);
    builder.rexec(keys);
    System.out.println(builder.getRf());
  }
}
