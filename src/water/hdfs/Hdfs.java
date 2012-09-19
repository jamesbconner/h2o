package water.hdfs;

import init.Init;
import water.H2O;
import water.Log;

public class Hdfs {
  private static final String DEFAULT_HDFS_VERSION = "cdh4";

  public static boolean initialize() {
    assert (H2O.OPT_ARGS.hdfs != null);
    if (H2O.OPT_ARGS.hdfs.equals("resurrect")) {
      throw new Error("HDFS resurrection is unimplemented");
    } else {
      // Load the HDFS backend for existing hadoop installations.
      // understands -hdfs=hdfs://server:port
      //             -hdfs-root=root
      //             -hdfs-config=config file
      String version = H2O.OPT_ARGS.hdfs_version==null ? DEFAULT_HDFS_VERSION : H2O.OPT_ARGS.hdfs_version;
      try {
        Init._init.addInternalJars("hadoop/"+version+"/");
      } catch(Exception e) {
        e.printStackTrace();
        Log.die("[hdfs] Unable to initialize hadoop version " + version +
            " please use different version.");
        return false;
      }
      PersistHdfs.ROOT.toString(); // Touch & thus start HDFS
      return true;
    }
  }
}
