package water.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import water.*;

/**
 *
 * @author peta
 */
public class PersistHdfs extends Persistence {

  @Override public Value load(Key k, Value sentinel) {
    assert (sentinel.type()=='I'); // we have only normal values at the moment that can reside in the HDFS
    VectorClock vc = VectorClock.NOW;
    long size = size(k,sentinel);
    if ((size> 2*ValueArray.chunk_size()) && k.user_allowed()) {
      ValueArray value = new ValueArray(k,size, vc.weak_vc(), vc.weak_jvmboot_time(),k._kb);
      value.setPersistenceBackend(this);
      value.makeChunks();
      return value;
    } else {
      Value value = new Value((int)size,0,vc.weak_vc(),vc.weak_jvmboot_time(),k,Value.PERSISTED);
      value.setPersistenceBackend(this);
      return value;
    }
  }
  
  @Override public byte[] get(Key k, Value v, int len) {
     try {
      FSDataInputStream s = null;
      try {
        s = _fs.open(getPathForKey(k));
        byte[] b = new byte[len];
        int br = s.read(getOffsetForKey(k),b, 0, len);
        assert (br == len);
        // the load was successful, check that it is still needed and update
        return b;
      } finally {
        if( s != null )
          s.close();
      }
    } catch( IOException e ) {
      return null;
    } 
  }

  @Override public boolean store(Key k, Value v) {
    int replica = k.replica(water.H2O.CLOUD);
    // replica numbers larger than 127 are not supported yet and default to
    // checking that desired replication factor has been achieved 
    if( replica < 0 ) {
      replica = k.desired();
    }
    // if we are home, do the persist normally
    if( replica == 0 ) {
      if (v instanceof ValueArray) // never store arraylets on HDFS, they are virtual
        return false;
      // on hdfs it is not possible to update arraylet chunks one at a time
      if (k.type()==Key.ARRAYLET_CHUNK)
        return false; // I believe this should be an error
      try {
        Path p = getPathForKey(k);
        _fs.mkdirs(p.getParent());
        FSDataOutputStream s = _fs.create(p);
        try {
          byte[] m = v.mem();
          if (m!=null) 
            s.write(m);
        } finally {
          s.close();
        }
        return true;
      } catch( IOException e ) {
        return false;
      }
    }
    // if we are something else determine our persistence based on the HDFS
    // replication reported for the file
    try {
      FileStatus fs = _fs.getFileStatus(getPathForKey(k));
      if( fs.getReplication() >= replica + 1 ) {
        return true;
      }
    } catch(IOException e) {
      return false;
    }
    return false;
  }

  @Override public boolean delete(Key k, Value v) {
    //assert (key.kind() == Key.HDFS_FILE);
    int replica = k.replica(water.H2O.CLOUD);
    // replica numbers larger than 127 are not supported yet and default to
    // checking that desired replication factor has been achieved 
    if( replica != 0 ) {
      return true;
    }
    // only the arraylet itself can delete the file from HDFS
    if (k.type()==Key.ARRAYLET_CHUNK)
      return true; // again, this might be an error

    /*else if (v instanceof ValueArray) { // not really here, done by the UKV guy
      ((ValueArray)v).deleteChunks();
      return true;
    } */
    Path p = getPathForKey(k);
    try {  _fs.delete(p, false); } // Try to delete, ignoring errors
    catch( IOException e ) { }
    try { return !_fs.exists(p); } // Check & return status but...
    catch( IOException e ) {
      return false; // Assume failure to delete if IO exception on 'exists' call
    }
  }

  @Override public long size(Key k, Value v) {
    try {
      long size = _fs.getFileStatus(getPathForKey(k)).getLen();
      if (k.type()==Key.ARRAYLET_CHUNK) {
        long offset = getOffsetForKey(k);
        size = (size-offset <=ValueArray.chunk_size()) ? size-offset : ValueArray.chunk_size();
      } 
      return size;
    } catch( IOException e ) {
      return 0;
    }
  }

  @Override public String name() {
    return "hdfs";
  }

  @Override public Type type() {
    return Type.HDFS;
  }
  
  private PersistHdfs() {
    super(Type.HDFS);
  }
  
  private final static Configuration _conf;
  private static FileSystem _fs;
  private static Path _root;
  
  static final String ROOT;
  public static final String DEFAULT_ROOT="ice";
  
  private static final PersistHdfs _instance;
  
  
  static {
    _conf = new Configuration();
    if (H2O.OPT_ARGS.hdfs_config!=null) {
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      _conf.addResource(p.getAbsolutePath());
      System.out.println("[hdfs] resource "+p.getAbsolutePath()+" added to the hadoop configuration");
      if (!H2O.OPT_ARGS.hdfs.isEmpty())
        System.err.println("[hdfs] connection server "+H2O.OPT_ARGS.hdfs+" from commandline ignored");
    } else {
      if (H2O.OPT_ARGS.hdfs.isEmpty())
        Log.die("[hdfs] you must specify the server to connect to. Use -hdfs=server:port");
      _conf.set("fs.default.name",H2O.OPT_ARGS.hdfs);
      System.out.println("[hdfs] fs.default.name = "+H2O.OPT_ARGS.hdfs);
    }
    ROOT = H2O.OPT_ARGS.hdfs_root==null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_root;
    System.out.println("[hdfs] hdfs root for H2O set to " + ROOT);
    try {
      _fs = FileSystem.get(_conf);
      _root = new Path(ROOT);
      _fs.mkdirs(_root);
    } catch( IOException e ) {
      // pass
      System.out.println(e.getMessage());
      Log.die("[hdfs] Unable to initialize persistency store home at " + ROOT);
    }
    _instance = new PersistHdfs();
  }
  
  public static void initialize() {
    System.out.println("[hdfs] loading persistent keys...");
    loadPersistentKeysFromFolder(_root,"");
  }
  
  
  private static void loadPersistentKeysFromFolder(Path folder, String prefix) {
    try {
      for( FileStatus f : _fs.listStatus(folder) ) {
        Path p = f.getPath();
        if( f.isDir() ) {
          // recursively also add files from the directories below
          loadPersistentKeysFromFolder(p, prefix + Path.SEPARATOR + p.getName());
        } else {
          // it is a file, therefore a value for us
          assert (_fs.isFile(p));
          Key k = decodeFile(p, prefix);
          if( k == null ) {
            continue;
          }
          Value val = _instance.getSentinel((short)0, (byte)'I');
          H2O.putIfAbsent_raw(k, val);
          k.is_local_persist(val,null); // Register knowledge of this key on disk
        }
      }
    } catch( IOException e ) {
      System.err.println("[hdfs] Unable to list the folder " + folder.toString());
    }
  }
  
  // Decodes the given file on a prefixed path to a key. 
  // TODO As of now does not support filenames (including prefix) larger than
  // 512 bytes.
  private static Key decodeFile(Path p, String prefix) {
    if( !prefix.isEmpty() ) {
      prefix = prefix.substring(1) + Path.SEPARATOR + p.getName(); // get rid of the leading '/'
    } else {
      prefix = p.getName();
    }
    assert (prefix.length() <= 512);
    // all HDFS keys are HDFS-kind keys
    return Key.make(prefix.getBytes());
  }
  
  
  // Returns the path for given key. 
  // TODO Something should be done about keys whose value is larger than 512
  // bytes, for now, they are not supported.
  static Path getPathForKey(Key k) {
    if (k.type()==Key.ARRAYLET_CHUNK)
      return new Path(_root, new String(k._kb).substring(10).replace(":","%37"));
    else
      return new Path(_root, new String(k._kb).replace(":","%37"));
  }
  
  // Returns the offset of the hdfsvalue (a non-arraylet value always returns
  // 0, arraylets return their respective offsets. 
  static long getOffsetForKey(Key k) {
    return (k.type()==Key.ARRAYLET_CHUNK) ? UDP.get8(k._kb,2) : 0;
  }
  
}
