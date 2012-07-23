package water.hdfs;

import init.Loader;
import water.H2O;
import water.Log;

/** Hdfs API class. 
 *
 * @author peta
 */
public class Hdfs {

  public static final String DEFAULT_HDFS_VERSION = "1.0.0";

  // initialization ------------------------------------------------------------  
  
  public static boolean initialize() {
    assert (H2O.OPT_ARGS.hdfs != null);
    
    if (H2O.OPT_ARGS.hdfs.equals("resurrect")) {
      // Loads the HDFS backend with ressurection mode. Experimental at the
      // moment
//      ValueBlock.initialize();
      
//      ValueINode.initialize();
      
      
    } else {
      // Load the HDFS backend for existing hadoop installations. 
      // understands -hdfs=hdfs://server:port
      //             -hdfs-root=root
      //             -hdfs-config=config file
      String version = DEFAULT_HDFS_VERSION;
      Loader loader = Loader.instance();
      if (!loader.addInternalJar("hadoop/"+version+"/hadoop-core-"+version+".jar"))
        Log.die("[hdfs] Unable to initialize hadoop version "+version+" please use different version.");
      new PersistHdfs();        // Touch & thus start HDFS
      return true;
    }
    return false;
  }
}
