package water;

import java.io.*;

// Persistence backend for the local file system.  
// Just for loading or storing files.
//
// @author cliffc
public abstract class PersistNFS {

  static final String KEY_PREFIX="nfs:";
  public static final int KEY_PREFIX_LENGTH=KEY_PREFIX.length();

  // file implementation -------------------------------------------------------
  public static Key decodeFile(File f) {
    String kname = KEY_PREFIX+File.separator+f.toString();
    assert (kname.length() <= 512);
    // all NFS keys are NFS-kind keys
    return Key.make(kname.getBytes());
  }
  
  // Returns the file for given key. 
  private static File getFileForKey(Key k) {
    final int len = KEY_PREFIX_LENGTH+1; // Strip key prefix & leading slash
    String s = new String(k._kb,len,k._kb.length-len);
    return new File(s);
  }

  // Read up to 'len' bytes of Value.  Value should already be persisted to
  // disk.  'len' should be sane: 0 <= len <= v._max (both ends are asserted
  // for, although it's hard to see the asserts).  A racing delete can trigger
  // a failure where we get a null return, but no crash (although one could
  // argue that a racing load&delete is a bug no matter what).
  static byte[] file_load(Value v, int len) {
    byte[] b = MemoryManager.allocateMemory(len);
    try {
      FileInputStream s = null;
      try {
        long skip = 0;
        Key k = v._key;
        // Convert an arraylet chunk into a long-offset from the base file.
        if( k._kb[0] == Key.ARRAYLET_CHUNK ) {
          skip = ValueArray.getOffset(k); // The offset
          k = Key.make(ValueArray.getArrayKeyBytes(k)); // From the base file key
        }
        s = new FileInputStream(getFileForKey(k));
        while( (skip -= s.skip(skip)) > 0 ) ; // Skip to offset
        for( int off = 0; off < len; off += s.read(b,off,len) ) ; // Read whole 'len'
        assert v.is_persisted();
        return b;
      } finally {
        if( s != null ) s.close();
      }
    } catch( IOException e ) {  // Broken disk / short-file???
      return null;
    } 
  }

  // Store Value v to disk.
  static void file_store(Value v) {
    // Only the home node does persistence on NFS
    if( !v._key.home() ) return;
    // A perhaps useless cutout: the upper layers should test this first.
    if( v.is_persisted() ) return;
    // Never store arraylets on NFS, instead we'll store the entire array.
    assert !(v instanceof ValueArray);
    try {
      File f = getFileForKey(v._key);
      f.mkdirs();
      FileOutputStream s = new FileOutputStream(f);
      try {
        byte[] m = v.mem();
        assert (m == null || m.length == v._max); // Assert not saving partial files 
        if( m!=null )
          s.write(m);
        v.setdsk();             // Set as write-complete to disk
      } finally {
        s.close();
      }
    } catch( IOException e ) {
    }
  }
  
  static void file_delete(Value v) {
    assert v._mem == null;      // Upper layers already cleared out
    assert !v.is_persisted();   // Upper layers already cleared out
    File f = getFileForKey(v._key);
    f.delete();
  }

  static Value lazy_array_chunk( Key key ) {
    assert key._kb[0] == Key.ARRAYLET_CHUNK;
    Key arykey = Key.make(ValueArray.getArrayKeyBytes(key)); // From the base file key
    long off = ValueArray.getOffset(key); // The offset
    long size = getFileForKey(arykey).length();
    long rem = size-off;
    int sz = (ValueArray.chunks(rem) > 1) ? (int)ValueArray.chunk_size() : (int)rem;
    Value val = new Value(sz,0,key,Value.NFS);
    val.setdsk();             // But its already on disk.
    return val;
  }
}
