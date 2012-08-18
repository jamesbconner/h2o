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
public class PersistIce extends Persistence {

  @Override public void store(Value v) { file_store(v);  }
  @Override public void delete(Value v) { file_delete(v); }
  @Override public byte[] load(Value v, int len) { return file_load(v,len); }

  public static byte INIT = IN_MEM+0;

  // initialization routines ---------------------------------------------------

  protected static final String ROOT;
  public static final String DEFAULT_ROOT = "/tmp";
  private static final String ICE_DIR = "ice";
  private static final File iceRoot;

  // Load into the K/V store all the files found on the local disk
  static {
    ROOT = (H2O.OPT_ARGS.ice_root==null) ? DEFAULT_ROOT : H2O.OPT_ARGS.ice_root;
    H2O.OPT_ARGS.ice_root = ROOT;
    iceRoot = new File(ROOT+File.separator+ICE_DIR+H2O.WEB_PORT);
    // Make the directory as-needed
    iceRoot.mkdirs();
    initializeFilesFromFolder(iceRoot);
  }

  // Initializes Key/Value pairs for files on the local disk.
  private static void initializeFilesFromFolder(File dir) {
    for (File f : dir.listFiles()) {
      if( f.isDirectory() ) {
        initializeFilesFromFolder(f); // Recursively keep loading K/V pairs
      } else {
        Key k = decodeKey(f);
        Value ice = Value.construct((int)f.length(),0,k,(byte)(ON_DISK+0),decodeType(f));
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
    } else {
      // system key, encoded by % and then 2 bytes for each byte of the key
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
  
  private static File encodeKeyToFile(Value v) {
    // check if we are system key
    Key k = v._key;
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
    return new File(iceRoot,getDirectoryForKey(v)+File.separator+sb.toString());
  }
  
  private static String getDirectoryForKey(Value v) {
    if( v._key._kb[0] != Key.ARRAYLET_CHUNK )
      return "not_an_arraylet";
    // Reverse arraylet key generation
    return new String(ValueArray.getArrayKeyBytes(v._key));
  }
  
  private byte[] file_load(Value v, int len) {
    synchronized(v) {           // Test under lock
      if( is_goal(v) == false || is(v)==false ) return null; // Trying to load mid-delete
    }
    // Allocate outside of lock
    byte[] b = MemoryManager.allocateMemory(len);
    synchronized(v) {           // File i/o under lock
      if( is_goal(v) == false || is(v)==false ) return null; // Trying to load mid-delete
      try {
        DataInputStream s = new DataInputStream(new FileInputStream(encodeKeyToFile(v)));
        try {
          s.readFully(b, 0, len);
          return b;
        } finally {
          s.close();
        }
      } catch( IOException e ) {  // Broken disk / short-file???
        //System.err.println(e.toString()+" for "+v._key+" and len="+len);
        return null;
      }
    }
  }
  
  private void file_store(Value v) {
    synchronized(v) {                  // Lock Value
      if( is_goal(v) == true ) return; // Some other thread is already trying to store
      assert is_goal(v) == false && is(v)==true; // State was: file-not-present
      set_info(v, 8);                            // Not-atomically set state to "store not-done"
      clr_info(v,16);
      assert is_goal(v) == true && is(v)==false; // State is: storing-not-done
      try {
        new File(iceRoot,getDirectoryForKey(v)).mkdirs();
        // Nuke any prior file.
        OutputStream s = new FileOutputStream(encodeKeyToFile(v));
        try {
          byte[] m = v.mem(); // we are not single threaded anymore 
          assert (m == null || m.length == v._max); // Assert not saving partial files
          if( m!=null )
            s.write(m);
        } finally {
          s.close();
          set_info(v,16);         // Set state to "store done"
          assert is_goal(v) == true && is(v)==true; // State is: store-done
        }
      } catch( IOException e ) {
        // Ignore IO errors, except that we never set state to "store done"
      }
    }
  }
  
  private void file_delete(Value v) {
    synchronized(v) {                  // Lock Value
      if( is_goal(v) == false ) return;         // Some other thread is already trying to remove
      clr_info(v, 8);                           // Not-atomically set state to "remove not-done"
      clr_info(v,16);
      assert is_goal(v) == false && is(v)==false; // State is: remove-not-done
      File f = encodeKeyToFile(v);
      f.delete();
      
      if( v instanceof ValueArray ) { // Also nuke directory if the top-level ValueArray dies
        f = new File(iceRoot,getDirectoryForKey(v));
        f.delete();
      }
      
      set_info(v,16);
      assert is_goal(v) == false && is(v)==true; // State is: remove-done
    }
  }
}
