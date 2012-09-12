package water.web;

import java.util.Properties;

import water.DKV;
import water.H2O;
import water.Key;
import water.UDP;
import water.Value;
import water.hdfs.TaskStore2HDFS;
import water.web.Page.PageError;

/**
 *
 * @author tomas
 */
public class Store2HDFS extends H2OPage {
  
  @Override public String serveImpl(Server s, Properties args) throws PageError {
    Key k = ServletUtil.check_key(args,"Key");
    Value v = DKV.get(k);    
    Key pK = Key.make(Key.make()._kb, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.SELF);    
    Value progress = new Value(pK,8);
    DKV.put(pK, progress);
    final long N = v.chunks();
    TaskStore2HDFS tsk = new TaskStore2HDFS(0,N,pK);
    tsk.invoke(k);
    // HTML file save of Value
    long storedCount = 0;
    while((storedCount = UDP.get8(DKV.get(pK).get(),0)) < N) {
      try {
        Thread.sleep(1000);
      } catch( InterruptedException e ) {
        throw new RuntimeException(e);        
      }
    } 
    // remove progress info
    DKV.remove(pK);
    return H2OPage.success("Successfully stored key <strong>"+k.toString()+"</strong> on HDFS");
  }

  @Override public String[] requiredArguments() {
    return new String[] { "Key" };
  }
}
