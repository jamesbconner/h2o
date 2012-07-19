package water;

import java.io.*;


/** Persistence backend for the local storage. 
 * 
 * Stores all keys as files, or if leveldb is enabled, stores values smaller
 * than arraylet chunk size in a leveldb format.
 * 
 * Metadata stored are the value type and the desired replication factor of the
 * key.
 *
 * @author peta
 */
public class PersistIce extends Persistence {

  private static final short SENTINEL_FILE = 0;
  
  @Override public Value load(Key k, Value sentinel) {
    VectorClock vc = VectorClock.NOW;
    Value result = null;
    switch (sentinel.type()) {
    case 'I': result = new Value((int)size(k,sentinel),0,vc.weak_vc(),vc.weak_jvmboot_time(),k,Value.PERSISTED); break;
    case 'A': result = new ValueArray((int)size(k,sentinel),0,vc.weak_vc(),vc.weak_jvmboot_time(),k,Value.PERSISTED); break;
    case 'C': result = new ValueCode((int)size(k,sentinel),0,vc.weak_vc(),vc.weak_jvmboot_time(),k,Value.PERSISTED); break;
    default:
      Log.die("[ice] unrecognized value type "+(char)sentinel.type()+" for key "+k.toString());
    }
    assert (result.type() == sentinel.type());
    result.setPersistenceBackend(this);
    return result;
  }

  @Override public byte[] get(Key k, Value v, int len) {
    return file_get(k,v,len);
  }

  @Override public boolean store(Key k, Value v) {
    return file_store(k,v);  
  }

  @Override public boolean delete(Key k, Value v) {
    return file_delete(k,v);
  }
  
  @Override public long size(Key k, Value v) {
    return file_size(k,v);
  }
  

  @Override public String name() {
    return "ice";
  }

  @Override public Type type() {
    return Type.ICE;
  }
  
  @Override public short sentinelData(Key k, Value v) {
    return SENTINEL_FILE;
  }
  
  
  
  
  // initialization routines ---------------------------------------------------

  protected static final String ROOT;
  public static final String DEFAULT_ROOT = "/tmp";
  
  private static final String ICE_DIR = "ice";

  private static final File iceRoot;
  
  private static final Persistence _instance;
  
  static {
    ROOT = (H2O.OPT_ARGS.ice_root==null) ? DEFAULT_ROOT : H2O.OPT_ARGS.ice_root;
    H2O.OPT_ARGS.ice_root = ROOT;
    iceRoot = new File(ROOT+File.separator+ICE_DIR+H2O.WEB_PORT);
    iceRoot.mkdirs();
    _instance = new PersistIce();
  }
 
  private PersistIce() {
    super(Type.ICE);
  }
 
  public static void initialize() {
    initializeFilesFromFolder(iceRoot);
  }  
  
  // Initializes the files on the local disk (values larger or equal in size to
  // the arraylet chunk size are stored as files. 
  private static void initializeFilesFromFolder(File dir) {
    for (File f:dir.listFiles()) {
      if (f.isDirectory()) {
        initializeFilesFromFolder(f);
      } else {
        Key k = decodeKey(f);
        Value ice = _instance.getSentinel(SENTINEL_FILE,decodeType(f));
        H2O.putIfAbsent_raw(k,ice);
        k.is_local_persist(ice,null);
      }
    }
  }
  
  public static Persistence instance() {
    return _instance;
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
    byte[] kb = null;
    // a normal key - ASCII with special characters encoded after % sign
    if ((key.length()<=2) || (key.charAt(0)!='%') || (key.charAt(1)<'0') || (key.charAt(1)>'9')) {
      byte[] nkb = new byte[key.length()];
      int j = 0;
      for( int i = 0; i < key.length(); ++i ) {
        byte b = (byte)key.charAt(i);
        if( b == '%' ) {
          switch( key.charAt(++i) ) {
          case '%':  b = '%' ; break;
          case 'b':  b = '\\'; break;
          case 'c':  b = ':' ; break;
          case 'd':  b = '.' ; break;
          case 's':  b = '/' ; break;
          default:   System.err.println("Invalid format of filename " + f.getName() + " at index " + i);
          }
        }
        nkb[j++] = b;
        kb = new byte[j];
        System.arraycopy(nkb,0,kb,0,j);
      }
    // system key, encoded by % and then 2 bytes for each byte of the key
    } else {
      kb = new byte[(key.length()-1)/2];
      int j = 0;
      // Then hexelate the entire thing
      for( int i = 1; i < key.length(); i+=2 ) {
        char b0 = (char)(key.charAt(i+0)-'0');
        if( b0 > 9 ) b0 += '0'+10-'A';
        char b1 = (char)(key.charAt(i+1)-'0');
        if( b1 > 9 ) b1 += '0'+10-'A';
        kb[j++] = (byte)((b0<<4)|b1);  // De-hexelated byte
      }
    }
    // now in kb we have the key name
    return Key.make(kb,decodeReplication(f));
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
  
  private static File encodeKeyToFile(Key k, Value v) {
    // check if we are system key
    StringBuilder sb = null;
    if (k._kb[0]<32) {
      sb = new StringBuilder(k._kb.length/2+4);
      sb.append('%');
      for( byte b : k._kb ) {
        int nib0 = ((b>>>4)&15)+'0';
        if( nib0 > '9' ) nib0 += 'A'-10-'0';
        int nib1 = ((b>>>0)&15)+'0';
        if( nib1 > '9' ) nib1 += 'A'-10-'0';
        sb.append((char)nib0).append((char)nib1);
      }
    // or a normal key
    } else {
      // Escapes special characters in the given key so that in can be used as a
      // filename on the disk
      sb = new StringBuilder(k._kb.length*2);
      for( byte b : k._kb ) {
        switch( b ) {
        case '%':  sb.append("%%"); break;
        case '.':  sb.append("%d"); break; // dot
        case '/':  sb.append("%s"); break; // slash
        case ':':  sb.append("%c"); break; // colon
        case '\\': sb.append("%b"); break; // backslash
        default:   sb.append((char)b); break;
        }
      }
    }
    // append the value type and replication factor
    sb.append('.');
    sb.append((char)v.type());
    sb.append(k.desired());
    return new File(iceRoot,getDirectoryForKey(k)+File.separator+sb.toString());
  }
  
  private static String getDirectoryForKey(Key k) {
    if ((k._kb.length < 10) || (k._kb[0]>=32)) {
      return "not_a_chunk";
    } else {
      long offset = UDP.get8(k._kb,2);
      return String.valueOf(offset / ValueArray.chunk_size());
    }
  }
  
  private byte[] file_get(Key k, Value v, int len) {
    try {
      File f = encodeKeyToFile(k,v);
      InputStream s = new FileInputStream(encodeKeyToFile(k,v));
      try {
        byte[] b = MemoryManager.allocateMemory(len);
        int br = s.read(b, 0, len);
        assert (br == len);
        // the load was successful, check that it is still needed and update
        assert b != null;
        return b;
      } finally {
        s.close();
      }
    } catch( IOException e ) {
      System.err.println(e.toString());
      return null;
    }
  }
  
  private boolean file_store(Key k, Value v) {
    try {
      // Nuke any prior file      
      new File(iceRoot,getDirectoryForKey(k)).mkdirs();
      OutputStream s = new FileOutputStream(encodeKeyToFile(k,v));
      try {
        byte[] m = v.mem(); // we are not single threaded anymore 
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
  
  private boolean file_delete(Key k, Value v) {
    if (v==null)
      return true;
    File f = encodeKeyToFile(k, v);
    f.delete();
    return !f.exists();
  }

  private long file_size(Key k, Value v) {
    return encodeKeyToFile(k, v).length();
  }
  
}
