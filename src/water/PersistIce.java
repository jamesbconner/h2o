package water;

import java.io.*;

// Persistence backend for the local storage device
//
// Stores all keys as files, or if leveldb is enabled, stores values smaller
// than arraylet chunk size in a leveldb format.
//
// Metadata stored are the value type and the desired replication factor of the
// key.
//
// @author peta, cliffc
public abstract class PersistIce {

  // initialization routines ---------------------------------------------------

  protected static final String ROOT;
  public static final String DEFAULT_ROOT = "/tmp";
  private static final String ICE_DIR = "ice";
  private static final File iceRoot;

  // Load into the K/V store all the files found on the local disk
  static void initialize() {}
  static {
    ROOT = (H2O.OPT_ARGS.ice_root==null) ? DEFAULT_ROOT : H2O.OPT_ARGS.ice_root;
    H2O.OPT_ARGS.ice_root = ROOT;
    iceRoot = new File(ROOT+File.separator+ICE_DIR+H2O.WEB_PORT);
    // Make the directory as-needed
    iceRoot.mkdirs();
    // By popular demand, clear out ICE on startup instead of trying to preserve it
    if( H2O.OPT_ARGS.keepice == null )  cleanIce(iceRoot);
    else initializeFilesFromFolder(iceRoot);
  }

  // Clear the ICE directory
  public static void cleanIce(File dir) {
    for( File f : dir.listFiles() ) {
      if( f.isDirectory() ) cleanIce(f); 
      f.delete();
    }
  }

  // Initializes Key/Value pairs for files on the local disk.
  private static void initializeFilesFromFolder(File dir) {
    for (File f : dir.listFiles()) {
      if( f.isDirectory() ) {
        initializeFilesFromFolder(f); // Recursively keep loading K/V pairs
      } else {
        Key k = decodeKey(f);
        Value ice = Value.construct((int)f.length(),0,k,Value.ICE,decodeType(f));
        ice.setdsk();
        H2O.putIfAbsent_raw(k,ice);
      }
    }
  }

  // file implementation -------------------------------------------------------

  // the filename can be either byte encoded if it starts with % followed by
  // a number, or is a normal key name with special characters encoded in
  // special ways.
  // It is questionable whether we need this because the only keys we have on
  // ice are likely to be arraylet chunks
  private static final Key decodeKey(File f) {
    String key = f.getName();
    key = key.substring(0,key.lastIndexOf('.')); 
    return Key.make(key,decodeReplication(f));
  }

  private static byte decodeReplication(File f) {
    String ext = f.getName();
    ext = ext.substring(ext.lastIndexOf('.')+1);
    try {
      return (byte)Integer.parseInt(ext.substring(1));
    } catch (NumberFormatException e) {
      Log.die("[ice] Unable to decode filename "+f.getAbsolutePath());
      return 0; // unreachable
    }
   }

  private static byte decodeType(File f) {
    String ext = f.getName();
    ext = ext.substring(ext.lastIndexOf('.')+1);
    return (byte)ext.charAt(0);
  }

  private static File encodeKeyToFile(Value v) {
    return encodeKeyToFile(v._key,v.type());
  }
  private static File encodeKeyToFile(Key k, byte type) {
    StringBuilder sb = new StringBuilder();
    // append the value type and replication factor */
    sb.append( k._kb[0] == Key.ARRAYLET_CHUNK ? (""+ValueArray.getChunkIndex(k)) : k.toString());
    sb.append('.');
    sb.append((char)type);
    sb.append(k.desired());
    return new File(iceRoot,getDirectoryForKey(k)+File.separator+sb.toString());
  }

  private static String getDirectoryForKey(Key key) {
    if( key._kb[0] != Key.ARRAYLET_CHUNK )
      return "not_an_arraylet";
    // Reverse arraylet key generation
    byte[] b = ValueArray.getArrayKeyBytes(key);
    int j=0;                    // Strip out ':' in directory names
    for( int i=0; i<b.length; i++ )
      if( b[i] != ':' )
        b[j++] = b[i];
    return new String(b,0,j);
  }

  // Read up to 'len' bytes of Value.  Value should already be persisted to
  // disk.  'len' should be sane: 0 <= len <= v._max (both ends are asserted
  // for, although it's hard to see the asserts).  A racing delete can trigger
  // a failure where we get a null return, but no crash (although one could
  // argue that a racing load&delete is a bug no matter what).
  static byte[] file_load(Value v, int len) {
    byte[] b = MemoryManager.allocateMemory(len);
    try {
      File f = encodeKeyToFile(v);
      if( f.length() < v._max ) { // Should be fully on disk... or
        System.out.println("Failed to file_load; file is short "+f.length());
        assert !v.is_persisted(); // or it's a racey delete of a spilled value
        return null;              // No value
      }
      DataInputStream s = new DataInputStream(new FileInputStream(f));
      try {
        s.readFully(b, 0, len);
        return b;
      } finally {
        s.close();
      }
    } catch( IOException e ) {  // Broken disk / short-file???
      return null;              // Also: EOFException for deleted files
    }
  }

  // Store Value v to disk.
  static void file_store(Value v) {
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.is_persisted() ) return;
    try {
      new File(iceRoot,getDirectoryForKey(v._key)).mkdirs();
      // Nuke any prior file.
      OutputStream s = new FileOutputStream(encodeKeyToFile(v));
      try {
        byte[] m = v._mem; // we are not single threaded anymore
        assert (m == null || m.length == v._max); // Assert not saving partial files
        if( m!=null )
          s.write(m);
        v.setdsk();             // Set as write-complete to disk
      } finally {
        s.close();
      }
    } catch( IOException e ) {
      throw new RuntimeException("File store failed: "+e);
    }
  }

  static void file_delete(Value v) {
    assert !v.is_persisted();   // Upper layers already cleared out
    File f = encodeKeyToFile(v);
    f.delete();
    if( v instanceof ValueArray ) { // Also nuke directory if the top-level ValueArray dies
      f = new File(iceRoot,getDirectoryForKey(v._key));
      f.delete();
    }
  }

  static Value lazy_array_chunk( Key key ) {
    assert key._kb[0] == Key.ARRAYLET_CHUNK;
    assert key.home();          // Only do this on the home node
    File f = encodeKeyToFile(key,Value.ICE);
    if( !f.isFile() ) return null;
    Value val = new Value((int)f.length(),0,key,Value.ICE);
    val.setdsk();               // But its already on disk.
    return val;
  }
}
