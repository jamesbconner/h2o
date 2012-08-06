package water.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import water.*;

// Persistence backend for HDFS
//
// @author peta, cliffc
public class PersistHdfs extends Persistence {

  @Override public void store(Value v) { file_store(v);  }
  @Override public void delete(Value v) { file_delete(v); }
  @Override public byte[] load(Value v, int len) { return file_load(v,len); }
  @Override public byte initial() { return ON_DISK; }

  // initialization routines ---------------------------------------------------

  static final String ROOT;
  public static final String DEFAULT_ROOT="ice";
  private final static Configuration _conf;
  private static FileSystem _fs;
  private static Path _root;
  
  // The _persistenceInfo byte given K/V's already on disk when JVM starts.
  private static final byte ON_DISK =
    (byte)(1/*type.HDFS.ordinal()*/ | // Persisted by the HDFS mechanism
           Persistence.ON_DISK | // Goal: persist object to disk & Goal is met
           0);                   // No more status bits needed
  
  static {
    if( H2O.OPT_ARGS.hdfs_config!=null ) {
      _conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      _conf.addResource(p.getAbsolutePath());
      System.out.println("[hdfs] resource "+p.getAbsolutePath()+" added to the hadoop configuration");
      if (!H2O.OPT_ARGS.hdfs.isEmpty())
        System.err.println("[hdfs] connection server "+H2O.OPT_ARGS.hdfs+" from commandline ignored");
    } else {
      if( H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty() ) {
        _conf = new Configuration();
        _conf.set("fs.default.name",H2O.OPT_ARGS.hdfs);
        System.out.println("[hdfs] fs.default.name = "+H2O.OPT_ARGS.hdfs);
      } else {
        _conf = null;
      }
    }
    ROOT = H2O.OPT_ARGS.hdfs_root==null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_root;
    if( H2O.OPT_ARGS.hdfs_config!=null || (H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty()) ) {
      System.out.println("[hdfs] hdfs root for H2O set to " + ROOT);
      try {
        _fs = FileSystem.get(_conf);
        _root = new Path(ROOT);
        _fs.mkdirs(_root);
        System.out.println("[hdfs] loading persistent keys...");
        int num = loadPersistentKeysFromFolder(_root,"");
        System.out.println("[hdfs] loaded "+num+" keys");
      } catch( IOException e ) {
        // pass
        System.out.println(e.getMessage());
        Log.die("[hdfs] Unable to initialize persistency store home at " + ROOT);
      }
    }
  }
  
  
  private static int loadPersistentKeysFromFolder(Path folder, String prefix) {
    int num=0;
    try {
      for( FileStatus f : _fs.listStatus(folder) ) {
        Path p = f.getPath();
        if( f.isDir() ) {
          // recursively also add files from the directories below
          num += loadPersistentKeysFromFolder(p, prefix + Path.SEPARATOR + p.getName());
        } else {
          // it is a file, therefore a value for us
          assert (_fs.isFile(p));
          Key k = decodeFile(p, prefix);
          if( k == null )
            continue;
          long size = _fs.getFileStatus(p).getLen();
          Value val = (size < 2*ValueArray.chunk_size())
            ? new Value((int)size,0,k,0)
            : new ValueArray(k,size);
          val._persistenceInfo = ON_DISK;
          H2O.putIfAbsent_raw(k, val);
          num++;
        }
      }
    } catch( IOException e ) {
      System.err.println("[hdfs] Unable to list the folder " + folder.toString());
    }
    return num;
  }
  
  // file implementation -------------------------------------------------------

  // Decodes the given file on a prefixed path to a key. 
  // TODO As of now does not support filenames (including prefix) larger than
  // 512 bytes.
  static private final String KEY_PREFIX="hdfs:/";
  static private final int KEY_PREFIX_LENGTH=KEY_PREFIX.length();
  private static Key decodeFile(Path p, String prefix) {
    String kname = KEY_PREFIX+prefix+Path.SEPARATOR+p.getName();
    assert (kname.length() <= 512);
    // all HDFS keys are HDFS-kind keys
    return Key.make(kname.getBytes());
  }
  
  // Returns the path for given key. 
  // TODO Something should be done about keys whose value is larger than 512
  // bytes, for now, they are not supported.
  static Path getPathForKey(Key k) {
    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
    String s = new String(k._kb,len,k._kb.length-len);
    return new Path(_root, s);
  }
  
  
  public synchronized byte[] file_load(Value v, int len) {
    if( is_goal(v) == false || is(v)==false ) return null; // Trying to load mid-delete
    if( v instanceof ValueArray )
      throw new Error("unimplemented: loading from arraylets");
    try {
      FSDataInputStream s = null;
      try {
        byte[] b = MemoryManager.allocateMemory(len);
        long off = 0;
        Key k = v._key;
        // Convert an arraylet chunk into a long-offset from the base file.
        if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
          off = ValueArray.getOffset(k); // The offset
          k = Key.make(ValueArray.getArrayKeyBytes(k)); // From the base file key
        }
        Path p = getPathForKey(k);
        s = _fs.open(p);
        int br = s.read(off, b, 0, len);
        assert (br == len);
        return b;
      } finally {
        if( s != null )
          s.close();
      }
    } catch( IOException e ) {
      return null;
    } 
  }

  public synchronized void file_store(Value v) {
    // Only the home node does persistence.
    if( !v._key.home() ) return;
    // Never store arraylets on HDFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);

    if( is_goal(v) == true ) return; // Some other thread is already trying to store
    assert is_goal(v) == false && is(v)==true; // State was: file-not-present
    set_info(v, 8);                            // Not-atomically set state to "store not-done"
    clr_info(v,16);
    assert is_goal(v) == true && is(v)==false; // State is: storing-not-done
    
    try {
      Path p = getPathForKey(v._key);
      _fs.mkdirs(p.getParent());
      FSDataOutputStream s = _fs.create(p);
      try {
        byte[] m = v.mem();
        if (m!=null) 
          s.write(m);
      } finally {
        s.close();
        set_info(v,16);       // Set state to "store done"
        assert is_goal(v) == true && is(v)==true; // State is: store-done
      }
    } catch( IOException e ) {
      // Ignore IO errors, except that we never set state to "store done"
    }
  }

  public void file_delete(Value v) {
    // Only the home node does persistence.
    if( !v._key.home() ) return;
    // Never store arraylets on HDFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);
    assert is_goal(v) == true && is(v)==true; // State was: store-done
    clr_info(v, 8);                           // Not-atomically set state to "remove not-done"
    clr_info(v,16);
    assert is_goal(v) == false && is(v)==false; // State is: remove-not-done

    Path p = getPathForKey(v._key);
    try { _fs.delete(p, false); } // Try to delete, ignoring errors
    catch( IOException e ) { }
    set_info(v,16);
    assert is_goal(v) == false && is(v)==true; // State is: remove-done
  }

}
