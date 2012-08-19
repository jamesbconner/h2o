package water.hdfs;

import init.Loader;
import water.H2O;
import water.Log;

/** Hdfs API class. 
 *
 * @author peta
 */
public class Hdfs {

  public static final String DEFAULT_HDFS_VERSION = "cdh4";

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
      String version = H2O.OPT_ARGS.hdfs_version==null ? DEFAULT_HDFS_VERSION : H2O.OPT_ARGS.hdfs_version;
      Loader loader = Loader.instance();
      // get all files inder jar folder 
      if (!loader.addInternalJarFolder("hadoop/"+version+"/",true))
        Log.die("[hdfs] Unable to initialize hadoop version "+version+" please use different version.");
      String dummy = PersistHdfs.ROOT; // Touch & thus start HDFS
      return true;
    }
    return false;
  }
}
