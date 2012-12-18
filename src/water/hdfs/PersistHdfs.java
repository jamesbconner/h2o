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

  static         final String KEY_PREFIX="hdfs:";
  static         final int    KEY_PREFIX_LEN = KEY_PREFIX.length();
  static         final int    HDFS_LEN;
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
      HDFS_LEN = H2O.OPT_ARGS.hdfs.length();
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
    } else {
      HDFS_LEN = 0;
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

  // file name implementation -------------------------------------------------
  // Convert Keys to Path Strings and vice-versa.  Assert this is a bijection.
  static Key getKeyForPathString(String str) {
    Key key = getKeyForPathString_impl(str);
    assert getPathStringForKey_impl(key).equals(str)
      : "hdfs name bijection: '"+str+"' makes key "+key+" makes '"+getPathStringForKey_impl(key)+"'";
    return key;
  }
  static private String getPathStringForKey(Key key) {
    String str = getPathStringForKey_impl(key);
    assert getKeyForPathString_impl(str) == key
      : "hdfs name bijection: key "+key+" makes '"+str+"' makes key "+getKeyForPathString_impl(str);
    return str;
  }
  // Actually we typically want a Path not a String
  private static Path getPathForKey(Key k) {  return new Path(getPathStringForKey(k)); }

  // The actual conversions; str->key and key->str
  // Convert string 'hdfs://192.168.1.151/datasets/3G_poker_shuffle'
  // into    key                    'hdfs:datasets/3G_poker_shuffle'
  static Key getKeyForPathString_impl(String str) {
    assert str.indexOf(H2O.OPT_ARGS.hdfs)==0 : str;
    return Key.make(KEY_PREFIX+str.substring(HDFS_LEN));
  }
  private static String getPathStringForKey_impl(Key k) {
    return H2O.OPT_ARGS.hdfs+new String(k._kb,KEY_PREFIX_LEN,k._kb.length-KEY_PREFIX_LEN);
  }

  // file implementation -------------------------------------------------------
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
