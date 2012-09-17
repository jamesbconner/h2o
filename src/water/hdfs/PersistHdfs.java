package water.hdfs;

import java.io.File;
import java.io.IOException;
import jsr166y.ForkJoinWorkerThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import water.*;

// Persistence backend for HDFS
//
// @author peta, cliffc
public abstract class PersistHdfs {

  // initialization routines ---------------------------------------------------

  static final String ROOT;
  public static final String DEFAULT_ROOT="ice";
  private final static Configuration _conf;
  private static FileSystem _fs;
  private static Path _root;

  static {
    if( H2O.OPT_ARGS.hdfs_config!=null ) {
      _conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[h2o,hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      _conf.addResource(p.getAbsolutePath());
      System.out.println("[h2o,hdfs] resource "+p.getAbsolutePath()+" added to the hadoop configuration");
    } else {
      if( H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty() ) {
        _conf = new Configuration();
        _conf.set("fs.defaultFS",H2O.OPT_ARGS.hdfs);
      } else {
        _conf = null;
      }
    }
    ROOT = H2O.OPT_ARGS.hdfs_root==null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_root;
    if( H2O.OPT_ARGS.hdfs_config!=null || (H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty()) ) {
      try {
        _fs = FileSystem.get(_conf);
        _root = new Path(ROOT);
        _fs.mkdirs(_root);
        int num = loadPersistentKeysFromFolder(_root,"");
        System.out.println("[h2o,hdfs] "+H2O.OPT_ARGS.hdfs+ROOT+" loaded "+num+" keys");
      } catch( IOException e ) {
        // pass
        System.out.println(e.getMessage());
        Log.die("[h2o,hdfs] Unable to initialize persistency store home at " + ROOT);
      }
    }
  }

  public static void refreshHDFSKeys(){
    if( _fs != null && _root != null ) loadPersistentKeysFromFolder(_root, "");
  }

  private static int loadPersistentKeysFromFolder(Path folder, String prefix) {
    // This code blocks alot, and does not have FJBlock support coded in
    assert !(Thread.currentThread() instanceof ForkJoinWorkerThread);
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
            ? new Value((int)size,0,k,Value.HDFS)
            : new ValueArray(k,size,Value.HDFS);
          val.setdsk();
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

  private static String path2KeyStr(Path p){
    if(p.getParent().depth() == _root.depth()) return KEY_PREFIX + Path.SEPARATOR + p.getName();
    return path2KeyStr(p.getParent()) + Path.SEPARATOR + p.getName();
  }

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
    final int len = KEY_PREFIX_LENGTH + 1; // Strip key prefix & leading slash
    String s = new String(k._kb,len, k._kb.length-len);
    return new Path(_root, s);
  }

  private static Key getKeyforPath(Path p){
    return Key.make(path2KeyStr(p));
  }


  static public byte[] file_load(Value v, int len) {
    byte[] b =  MemoryManager.allocateMemory(len);
    try {
      FSDataInputStream s = null;
      try {
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
        assert v.is_persisted();
        return b;
      } finally {
        if( s != null )
          s.close();
      }
    } catch( IOException e ) {  // Broken disk / short-file???
      System.out.println("hdfs file_load throws error "+e+" for key "+v._key);
      return null;
    }
  }

//for moving ValueArrays to HDFS
 static void storeChunk(Value v, String path) {
   assert !(v instanceof ValueArray);
   try {
     Path p = new Path(_root, path);
     FSDataOutputStream s;
     if( (v._key._kb[0] != Key.ARRAYLET_CHUNK)
         || (ValueArray.getChunkIndex(v._key) == 0) ) {
       // the first chunk -> make sure path exists and
       // create/overwrite the file
       _fs.mkdirs(p.getParent());
       s = _fs.create(p);
     } else
       s = _fs.append(p);
     try {
       // we're moving file to hdfs, possibly from other source, make sure it
       // is loaded first
       byte[] m = v.get();
       if( m != null )
         s.write(m);
     } finally {
       s.close();
     }
   } catch( IOException e ) {
     e.printStackTrace();
   }
 }


 static void addNewVal2KVStore(String path){
   Path p = new Path(_root,path);
   Key k = getKeyforPath(p);
   try{
     if(!_fs.isFile(p))throw new Error("No such file on hdfs! " + path);
     long size = _fs.getFileStatus(p).getLen();
     Value val = (size < 2 * ValueArray.chunk_size()) ? new Value(
         (int) size, 0, k, Value.HDFS) : new ValueArray(k, size,
         Value.HDFS);
     val.setdsk();
     H2O.putIfAbsent_raw(k, val);
   }catch(IOException e){throw new Error(e);}
 }


  static public void file_store(Value v) {
    // Only the home node does persistence on HDFS
    if( !v._key.home() ) return;
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.is_persisted() ) return;
    // Never store arraylets on HDFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);
    try {
      Path p = getPathForKey(v._key);
      _fs.mkdirs(p.getParent());
      FSDataOutputStream s = _fs.create(p);
      try {
        byte[] m = v.mem();
        assert (m == null || m.length == v._max); // Assert not saving partial files
        if (m!=null)
          s.write(m);
        v.setdsk();             // Set as write-complete to disk
      } finally {
        s.close();
      }
    } catch( IOException e ) {
    }
  }

  static public void file_delete(Value v) {
    // Only the home node does persistence.
    if(v._key._kb[0] == Key.ARRAYLET_CHUNK) return;
    if( !v._key.home() ) return;

    // Never store arraylets on HDFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);
    assert !v.is_persisted();   // Upper layers already cleared out
    Path p = getPathForKey(v._key);
    try { _fs.delete(p, false); } // Try to delete, ignoring errors
    catch( IOException e ) { }
  }

  public static Value lazy_array_chunk( Key key ) {
    assert key._kb[0] == Key.ARRAYLET_CHUNK;
    try {
      Key arykey = Key.make(ValueArray.getArrayKeyBytes(key)); // From the base file key
      long off = ValueArray.getOffset(key); // The offset
      Path p = getPathForKey(arykey);
      long size = _fs.getFileStatus(p).getLen();
      long rem = size-off;
      int sz = (ValueArray.chunks(rem) > 1) ? (int)ValueArray.chunk_size() : (int)rem;

      // the last chunk can be fat, so it got packed into the earlier chunk
      if( rem < ValueArray.chunk_size() && off > 0 ) return null;
      Value val = new Value(sz,0,key,Value.HDFS);
      val.setdsk();               // But its already on disk.
      return val;
    } catch( IOException e ) {  // Broken disk / short-file???
      return null;
    }
  }
}
