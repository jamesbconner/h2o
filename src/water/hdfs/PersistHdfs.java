package water.hdfs;

import water.*;
import java.io.*;
import jsr166y.ForkJoinWorkerThread;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

// Persistence backend for HDFS
//
// @author peta, cliffc
public abstract class PersistHdfs {

  static final String KEY_PREFIX="hdfs:";
  public static final int KEY_PREFIX_LENGTH=KEY_PREFIX.length();

  static private final Configuration _conf;
  static private       FileSystem _fs;
  static         final String ROOT;

  static void initialize() {}
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
    ROOT = H2O.OPT_ARGS.hdfs_root == null ? "ice" : H2O.OPT_ARGS.hdfs_root;
    if( H2O.OPT_ARGS.hdfs_config != null || (H2O.OPT_ARGS.hdfs != null && !H2O.OPT_ARGS.hdfs.isEmpty()) ) {
      try {
        _fs = FileSystem.get(_conf);
        if (H2O.OPT_ARGS.hdfs_nopreload==null) {
          // This code blocks alot, and does not have FJBlock support coded in
          assert !(Thread.currentThread() instanceof ForkJoinWorkerThread);
          int num = addFolder(new Path(ROOT));
          System.out.println("[h2o,hdfs] " + H2O.OPT_ARGS.hdfs+ROOT+" loaded " + num + " keys");
        }
      } catch( IOException e ) {
        // pass
        System.out.println(e.getMessage());
        Log.die("[h2o,hdfs] Unable to initialize persistency store home at " + ROOT);
      }
    }
  }

  private static int addFolder(Path p) {
    int num=0;
    try {
      for( FileStatus fs : _fs.listStatus(p) ) {
        Path pfs = fs.getPath();
        if( fs.isDir() ) {
          num += addFolder(pfs);
        } else {
          num++;
          Key k = getKeyForPathString(pfs.toString());
          long size = fs.getLen();
          Value val = null;
          if( pfs.getName().endsWith(".hex") ) { // Hex file?
            FSDataInputStream s = _fs.open(pfs);
            int sz = s.readShort();
            if( sz <= 0 ) {
              System.err.println("Invalid hex file: " + pfs);
              continue;
            }
            byte [] mem = MemoryManager.malloc1(sz);
            s.readFully(mem);
            ValueArray ary = new ValueArray(k,size,Value.HDFS).read(new AutoBuffer(mem));
            val = ary.value();
          } else if( size >= 2*ValueArray.CHUNK_SZ ) {
            val = new ValueArray(k,size,Value.HDFS).value(); // ValueArray byte wrapper over a large file
          } else {
            val = new Value(k,(int)size,Value.HDFS); // Plain Value
          }
          val.setdsk();
          H2O.putIfAbsent_raw(k,val);
        }
      }
    } catch( IOException e ) {
      e.printStackTrace();
      System.err.println("[hdfs] Unable to list the folder " + p.toString());
    }
    return num;
  }
  
  public static int pad8(int x){
    return (x == (x & 7))?x:(x & ~7) + 8;
  }
  public static boolean createHexHeader(Value v, String path) {
    try {
      FSDataOutputStream s=null;
      try {
        Path p = new Path(ROOT, path);
        _fs.mkdirs(p.getParent());
        s = _fs.create(p);
        if( v._isArray != 0 && path.endsWith(".hex") ) {
          byte [] mem  = v.get();
          int padding = pad8(mem.length + 2) - mem.length - 2;
          // write length of the header in bytes
          s.writeShort((short)(mem.length+padding));
          // write the header data
          s.write(mem);
          // pad the length to multiple of 8
          for(int i = 0; i < padding; ++i)s.writeByte(0);
        }
        return true;
      } finally {
        if( s != null ) s.close();
      }
    } catch( IOException e ) {
      e.printStackTrace();
      return false;
    }
  }

  // for moving ValueArrays to HDFS
  static boolean storeChunk(byte[] m, String path) {
    try {
      FSDataOutputStream s = null;
      try {
        Path p = new Path(ROOT, path);
        s = _fs.append(p);
        s.write(m);
        return true;
      } finally {
        if( s != null ) s.close();
      }
    } catch( IOException e ) {
      e.printStackTrace();
      return false;
    }
  }

  // file implementation -------------------------------------------------------
  public static Key getKeyForPathString(String str) { return Key.make(str); }

  // Returns the path for given key.
  private static Path getPathForKey(Key k) {
    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
    String s = new String(k._kb,len,k._kb.length-len);
    return new Path("hdfs:/"+s);
  }

  // Read up to 'len' bytes of Value. Value should already be persisted to
  // disk. 'len' should be sane: 0 <= len <= v._max (both ends are asserted
  // for, although it's hard to see the asserts). A racing delete can trigger
  // a failure where we get a null return, but no crash (although one could
  // argue that a racing load&delete is a bug no matter what).
  public static byte[] file_load(Value v) {
    byte[] b = MemoryManager.malloc1(v._max);
    try {
      FSDataInputStream s = null;
      try {
        long skip = 0;
        Key k = v._key;
        // Convert an arraylet chunk into a long-offset from the base file.
        if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
          skip = ValueArray.getChunkOffset(k); // The offset
          k = ValueArray.getArrayKey(k);       // From the base file key
        }
        s = _fs.open(getPathForKey(k));
        while( (skip -= s.skip(skip)) > 0 ) ; // Skip to offset
        for( int off = 0; off < v._max; off += s.read(b,off,v._max) ) ; // Read whole
        assert v.is_persisted();
        return b;
      } finally {
        if( s != null ) s.close();
      }
    } catch( IOException e ) { // Broken disk / short-file???
      System.out.println(e);
      return null;
    }
  }

  // Store Value v to disk.
  public static void file_store(Value v) {
    // Only the home node does persistence on NFS
    if( !v._key.home() ) return;
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.is_persisted() ) return;
    // Never store arraylets on NFS, instead we'll store the entire array.
    assert v._isArray==0;
    throw H2O.unimpl();
    //try {
    //  Path p = getPathForKey(v._key);
    //  f.mkdirs();
    //  FSDataOutputStream s = new FSDataOutputStream(f);
    //  try {
    //    byte[] m = v.mem();
    //    assert (m == null || m.length == v._max); // Assert not saving partial files
    //    if( m!=null )
    //      s.write(m);
    //    v.setdsk(); // Set as write-complete to disk
    //  } finally {
    //    s.close();
    //  }
    //} catch( IOException e ) {
    //}
  }

  static public void file_delete(Value v) {
    assert !v.is_persisted(); // Upper layers already cleared out
    throw H2O.unimpl();
    //File f = getFileForKey(v._key);
    //f.delete();
  }

  static public Value lazy_array_chunk( Key key ) {
    Key arykey = ValueArray.getArrayKey(key);  // From the base file key
    long off = ValueArray.getChunkOffset(key); // The offset
    long size = 0;
    try { 
      size = _fs.getFileStatus(getPathForKey(arykey)).getLen();
    } catch( IOException e ) {
      System.out.println(e);
      return null;
    }
    long rem = size-off;

    // the last chunk can be fat, so it got packed into the earlier chunk
    if( rem < ValueArray.CHUNK_SZ && off > 0 ) return null;
    int sz = (rem >= ValueArray.CHUNK_SZ*2) ? (int)ValueArray.CHUNK_SZ : (int)rem;
    Value val = new Value(key,sz,Value.HDFS);
    val.setdsk(); // But its already on disk.
    return val;
  }
}
  
//  
//  public static String getHDFSRoot() {
//    return _root == null ? null : H2O.OPT_ARGS.hdfs + _root.toString();
//  }
//
//  public static int pad8(int x){
//    return (x == (x & 7))?x:(x & ~7) + 8;
//  }
//  public static void refreshHDFSKeys() {
//    if( (_fs != null) && (_root != null) && (H2O.OPT_ARGS.hdfs_nopreload==null) ) loadPersistentKeysFromFolder(_root, "");
//  }
//
//
//  public static Value readValueFromFile(Path p, String prefix) throws IOException {
//    Key k = decodeFile(p, prefix);
//    
//    if( k == null )
//      return null;
//    if(!_fs.isFile(p)) throw new IOException("No such file on hdfs! " + p);
//    long size = _fs.getFileStatus(p).getLen();
//    Value val;
//    if(p.getName().endsWith(".hex")){
//      FSDataInputStream s = _fs.open(p);
//      int sz = s.readShort();
//      if(sz <= 0){
//        System.err.println("Invalid hex file: " + p);
//        return null;
//      }
//      byte [] mem = MemoryManager.malloc1(sz);
//      s.readFully(mem);
//      val = new ValueArray(k,mem, Value.HDFS);
//      //val.switch2HdfsBackend(true);
//    } else {
//       val = (size < 2*ValueArray.chunk_size())
//          ? new Value((int)size,0,k,Value.HDFS)
//          : new ValueArray(k,size,Value.HDFS);
//       val.setdsk();
//    }
//    H2O.putIfMatch(k, val, null);
//    return val;
//  }
//
//  // file implementation -------------------------------------------------------
//
//  // Decodes the given file on a prefixed path to a key.
//  // TODO As of now does not support filenames (including prefix) larger than
//  // 512 bytes.
//  static private final String KEY_PREFIX="hdfs:/";
//  static private final int KEY_PREFIX_LENGTH=KEY_PREFIX.length();
//
//  public static String path2KeyStr(Path p) {
//    if(p.depth() == _root.depth()) return KEY_PREFIX + Path.SEPARATOR;
//    if(p.getParent().depth() == _root.depth()) return KEY_PREFIX + Path.SEPARATOR + p.getName();
//    return path2KeyStr(p.getParent()) + Path.SEPARATOR + p.getName();
//  }
//
//  private static Key decodeFile(Path p, String prefix) {
//    String kname = KEY_PREFIX+prefix+Path.SEPARATOR+p.getName();
//    assert (kname.length() <= 512);
//    // all HDFS keys are HDFS-kind keys
//    return Key.make(kname.getBytes());
//  }
//
//  // Returns the path for given key.
//  // TODO Something should be done about keys whose value is larger than 512
//  // bytes, for now, they are not supported.
//  static Path getPathForKey(Key k) {
//    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
//    String s = new String(k._kb,len, k._kb.length-len);
//    return new Path(_root, s);
//  }
//
//  public static Key getKeyForPath(String p){
//    return Key.make(KEY_PREFIX + Path.SEPARATOR + p);
//  }
//
//  static public byte[] file_load(Value v, int len) {
//    byte[] b =  MemoryManager.malloc1(len);
//    try {
//      FSDataInputStream s = null;
//      try {
//        long off = 0;
//        Key k = v._key;
//        Path p;
//        // Convert an arraylet chunk into a long-offset from the base file.
//        if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
//          Key hk = Key.make(ValueArray.getArrayKeyBytes(k)); // From the base file key
//          p = getPathForKey(hk);
//          ValueArray ary = (ValueArray)DKV.get(hk);
//          off = ary.getChunkFileOffset(k);
//        } else
//          p = getPathForKey(k);
//        try {
//          s = _fs.open(p);
//        } catch (IOException e) {
//          if (e.getMessage().equals("Filesystem closed")) {
//            System.out.println("Retrying fs.open with a new FileSystem to fix the too-many-open-files problem");
//            _fs = FileSystem.get(_conf);
//            s = _fs.open(p);
//          } else {
//            throw e;
//          }
//        }
//        int br = s.read(off, b, 0, len);
//        assert (br == len);
//        assert v.is_persisted();
//      } finally {
//        if( s != null )
//          s.close();
//      }
//      return b;
//    } catch( IOException e ) {  // Broken disk / short-file???
//      System.out.println("hdfs file_load throws error "+e+" for key "+v._key);
//      return null;
//    }
//  }
//
//
////for moving ValueArrays to HDFS
// static void storeChunk(Value v, String path) throws IOException {
//     Path p = new Path(_root, path);
//     FSDataOutputStream s = _fs.append(p);
//     try {
//       // we're moving file to hdfs, possibly from other source, make sure it
//       // is loaded first
//       byte[] m = v.get();
//       if( m != null ) s.write(m);
//     } finally {
//       s.close();
//     }
// }
//
// public static Key importPath(String path) throws IOException {
//   Path p = new Path(_root,path);
//   return readValueFromFile(p,path2KeyStr(p.getParent()))._key;
// }
//
// public static Key importPath(String path, String prefix) throws IOException {
//   Path p = new Path(_root,path);
//   return readValueFromFile(p,prefix)._key;
// }
//
//  static public void file_store(Value v) {
//    // Only the home node does persistence on HDFS
//    if( !v._key.home() ) return;
//    // A perhaps useless cutout: the upper layers should test this first.
//    if( v.is_persisted() ) return;
//    // Never store arraylets on HDFS, instead we'll store the entire array.
//    assert !(v instanceof ValueArray);
//    // individual arraylet chunks can not be on hdfs
//    // (hdfs files can not be written to at specified offset)
//    assert v._key._kb[0] != Key.ARRAYLET_CHUNK;
//
//    try {
//      Path p = getPathForKey(v._key);
//      _fs.mkdirs(p.getParent());
//      FSDataOutputStream s = _fs.create(p);
//      try {
//        byte[] m = v.mem();
//        assert (m == null || m.length == v._max); // Assert not saving partial files
//        if (m!=null)
//          s.write(m);
//        v.setdsk();             // Set as write-complete to disk
//      } finally {
//        s.close();
//      }
//    } catch( IOException e ) {
//      e.printStackTrace();
//    }
//  }
//
//  static public void file_delete(Value v) {
//    // Only the home node does persistence.
//    if(v._key._kb[0] == Key.ARRAYLET_CHUNK) return;
//    if( !v._key.home() ) return;
//
//    // Never store arraylets on HDFS, instead we'll store the entire array.
//    assert !(v instanceof ValueArray);
//    assert !v.is_persisted();   // Upper layers already cleared out
//    Path p = getPathForKey(v._key);
//    try { _fs.delete(p, false); } // Try to delete, ignoring errors
//    catch( IOException e ) { }
//  }
//
//  public static Value lazy_array_chunk( Key key ) {
//    assert key._kb[0] == Key.ARRAYLET_CHUNK;
//    try {
//      Key arykey = Key.make(ValueArray.getArrayKeyBytes(key)); // From the base file key
//      ValueArray ary = (ValueArray)DKV.get(arykey);
//      long off = ary.getChunkFileOffset(key);
//      Path p = getPathForKey(arykey);
//      long size = _fs.getFileStatus(p).getLen();
//      long rem = size-off;
//      // We have to get real size of chunk
//      int sz = (ValueArray.chunks(rem) > 1) ? (int)ary.chunk_size_structured() : (int)rem;
//
//      // The last chunk can be fat, so it got packed into the earlier chunk, but ONLY if there is more than one chunk
//      if( rem < ary.chunk_size_structured() && off > 0 && ary.chunks() > 1) return null;
//
//      Value val = new Value(sz,0,key,Value.HDFS);
//      val.setdsk();               // But its already on disk.
//      return val;
//    } catch( IOException e ) {  // Broken disk / short-file???
//      System.out.println("[hdfs] PersistHdfs.lazy_array_chunk() - returning null, because cannot load chunk lazily due to:" + e.getMessage());
//      return null;
//    }
//  }
//}
