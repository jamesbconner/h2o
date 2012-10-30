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
  
  
  public static String getHDFSRoot() {
    return _root == null ? null : H2O.OPT_ARGS.hdfs + _root.toString();
  }

  public static int pad8(int x){
    return (x == (x & 7))?x:(x & ~7) + 8;
  }
  static {
    if( H2O.OPT_ARGS.hdfs_config!=null ) {
      _conf = new Configuration();
      File p = new File(H2O.OPT_ARGS.hdfs_config);
      if (!p.exists())
        Log.die("[h2o,hdfs] Unable to open hdfs configuration file "+p.getAbsolutePath());
      _conf.addResource(p.getAbsolutePath());
      System.out.println("[h2o,hdfs] resource " + p.getAbsolutePath() + " added to the hadoop configuration");
    } else {
      if( H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty() ) {
        _conf = new Configuration();
        _conf.set("fs.defaultFS",H2O.OPT_ARGS.hdfs);
      } else {
        _conf = null;
      }
    }
    ROOT = H2O.OPT_ARGS.hdfs_root == null ? DEFAULT_ROOT : H2O.OPT_ARGS.hdfs_root;
    if( H2O.OPT_ARGS.hdfs_config != null || (H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty()) ) {
      try {
        _fs = FileSystem.get(_conf);
        _root = new Path(ROOT);
        _fs.mkdirs(_root);
        if (H2O.OPT_ARGS.hdfs_nopreload==null) {
          int num = loadPersistentKeysFromFolder(_root,"");
          System.out.println("[h2o,hdfs] " + H2O.OPT_ARGS.hdfs+ROOT+" loaded " + num + " keys");
        }
      } catch( IOException e ) {
        // pass
        System.out.println(e.getMessage());
        Log.die("[h2o,hdfs] Unable to initialize persistency store home at " + ROOT);
      }
    }
  }

  public static void refreshHDFSKeys() {
    if( (_fs != null) && (_root != null) && (H2O.OPT_ARGS.hdfs_nopreload==null) ) loadPersistentKeysFromFolder(_root, "");
  }


  public static Value readValueFromFile(Path p, String prefix) throws IOException {
    Key k = decodeFile(p, prefix);
    
    if( k == null )
      return null;
    if(!_fs.isFile(p)) throw new IOException("No such file on hdfs! " + p);
    long size = _fs.getFileStatus(p).getLen();
    Value val;
    if(p.getName().endsWith(".hex")){
      FSDataInputStream s = _fs.open(p);
      int sz = s.readShort();
      if(sz <= 0){
        System.err.println("Invalid hex file: " + p);
        return null;
      }
      byte [] mem = MemoryManager.allocateMemory(sz);
      s.readFully(mem);
      val = new ValueArray(k,mem, Value.HDFS);
      //val.switch2HdfsBackend(true);
    } else {
       val = (size < 2*ValueArray.chunk_size())
          ? new Value((int)size,0,k,Value.HDFS)
          : new ValueArray(k,size,Value.HDFS);
       val.setdsk();
    }
    H2O.putIfMatch(k, val, null);
    return val;
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
          readValueFromFile(p, prefix);
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

  public static String path2KeyStr(Path p) {
    if(p.depth() == _root.depth()) return KEY_PREFIX + Path.SEPARATOR;
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
    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
    String s = new String(k._kb,len, k._kb.length-len);
    return new Path(_root, s);
  }

  public static Key getKeyForPath(String p){
    return Key.make(KEY_PREFIX + Path.SEPARATOR + p);
  }

  static public byte[] file_load(Value v, int len) {
    byte[] b =  MemoryManager.allocateMemory(len);
    try {
      FSDataInputStream s = null;
      try {
        long off = 0;
        Key k = v._key;
        Path p;
        // Convert an arraylet chunk into a long-offset from the base file.
        if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
          Key hk = Key.make(ValueArray.getArrayKeyBytes(k)); // From the base file key
          p = getPathForKey(hk);
          ValueArray ary = (ValueArray)DKV.get(hk);
          off = ary.getChunkFileOffset(k);
        } else
          p = getPathForKey(k);
        try {
          s = _fs.open(p);
        } catch (IOException e) {
          if (e.getMessage().equals("Filesystem closed")) {
            _fs = FileSystem.get(_conf);
            s = _fs.open(p);
          } else {
            throw e;
          }
        }
        int br = s.read(off, b, 0, len);
        assert (br == len);
        assert v.is_persisted();
      } finally {
        if( s != null )
          s.close();
      }
      return b;
    } catch( IOException e ) {  // Broken disk / short-file???
      System.out.println("hdfs file_load throws error "+e+" for key "+v._key);
      return null;
    }
  }


  static void createFile(Value v, String path) throws IOException{
    Path p = new Path(_root, path);
    FSDataOutputStream s;
    _fs.mkdirs(p.getParent());
    s = _fs.create(p);
    try {
      if((v instanceof ValueArray) && path.endsWith(".hex")){
        byte [] mem  = v.get();
        int padding = pad8(mem.length + 2) - mem.length - 2;
        // write lenght of the header in bytes
        s.writeShort((short)(mem.length+padding));
        // write the header data
        s.write(mem);
        // pad the lenght to multiple of 8
        for(int i = 0; i < padding; ++i)s.writeByte(0);
      }
    }finally{
      s.close();
    }
  }
//for moving ValueArrays to HDFS
 static void storeChunk(Value v, String path) throws IOException {
     Path p = new Path(_root, path);
     FSDataOutputStream s = _fs.append(p);
     try {
       // we're moving file to hdfs, possibly from other source, make sure it
       // is loaded first
       byte[] m = v.get();
       if( m != null ) s.write(m);
     } finally {
       s.close();
     }
 }

 public static Key importPath(String path) throws IOException {
   Path p = new Path(_root,path);
   return readValueFromFile(p,path2KeyStr(p.getParent()))._key;
 }

 public static Key importPath(String path, String prefix) throws IOException {
   Path p = new Path(_root,path);
   return readValueFromFile(p,prefix)._key;
 }

  static public void file_store(Value v) {
    // Only the home node does persistence on HDFS
    if( !v._key.home() ) return;
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.is_persisted() ) return;
    // Never store arraylets on HDFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);
    // individual arraylet chunks can not be on hdfs
    // (hdfs files can not be written to at specified offset)
    assert v._key._kb[0] != Key.ARRAYLET_CHUNK;

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
      e.printStackTrace();
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
      ValueArray ary = (ValueArray)DKV.get(arykey);
      long off = ary.getChunkFileOffset(key);
      Path p = getPathForKey(arykey);
      long size = _fs.getFileStatus(p).getLen();
      long rem = size-off;
      // We have to get real size of chunk
      int sz = (ValueArray.chunks(rem) > 1) ? (int)ary.chunk_size_structured() : (int)rem;

      // The last chunk can be fat, so it got packed into the earlier chunk, but ONLY if there is more than one chunk
      if( rem < ary.chunk_size_structured() && off > 0 && ary.chunks() > 1) return null;

      Value val = new Value(sz,0,key,Value.HDFS);
      val.setdsk();               // But its already on disk.
      return val;
    } catch( IOException e ) {  // Broken disk / short-file???
      System.out.println("[hdfs] PersistHdfs.lazy_array_chunk() - returning null, because cannot load chunk lazily due to:" + e.getMessage());
      return null;
    }
  }
}
