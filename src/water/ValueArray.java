package water;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Large Arrays & Arraylets
 *
 * Large arrays are broken into 1Meg chunks (except the last chunk which may be
 * from 1 to 2Megs).
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */


public class ValueArray extends Value {

  public static final int LOG_CHK = 20; // Chunks are 1<<20, or 1Meg

  private void initialize(long sz, byte[] uid) {
    UUID uuid = UUID.randomUUID();
    mem()[0] = 0;
    mem()[1] = 0;
    UDP.set8(mem(), 2,sz);
    if (uid==null) {
      UDP.set8(mem(), 10,uuid.getLeastSignificantBits());
      UDP.set8(mem(), 18,uuid. getMostSignificantBits());
    } else {
      System.arraycopy(uid, 0, mem(), 10, uid.length);
    }
  }
  
  public ValueArray( Key key, long sz ) {
    // The _mem bits:
    //  0- 1 - 0,0; 0==> system key, 0==> no IP home forcing
    //  2- 9 - long size
    // 10-17 - UUID low
    // 18-25 - UUID high
    super(key,2+8+16);
    initialize(sz,null);
  }
  
  public ValueArray( int max, int len, Key key, int mode ) {
    super(max,len,key,mode);
  }
  
  @Override public long length() { return UDP.get8(get(),2); }

  @Override public byte type() { return ARRAYLET; }

  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[array] size=").append(length());
    return true;
  }

  // Make a system chunk-key from an offset into the main array
  public Key make_chunkkey( long off ) {
    byte[] kb = get().clone();  // Make a copy of the main Value array
    UDP.set8(kb,2,off);         // Blast down the offset.
    return Key.make(kb,(byte)_key.desired()); // Presto!  A unique arraylet key!
  }

  // Number of chunks in this array
  // Divide by 1Meg into chunks.  The last chunk is between 1 and 2 megs
  static private int chunks(long sz) { return (int)(sz>>LOG_CHK); }
  @Override public int chunks() { return chunks(length()); }

  // Convert a chunk-number into a long offset
  @Override public long chunk_offset( int chunknum ) { return (long)chunknum << LOG_CHK; }
  // Get a Key for the chunk; fetching the Value for this Key gets this chunk
  @Override public Key chunk_get( int chunknum ) {
    if( chunknum < 0 || chunknum >= chunks() )
      throw new ArrayIndexOutOfBoundsException(chunknum);
    return make_chunkkey(chunk_offset(chunknum));
  }

  public static long chunk_size() {
    return 1 << LOG_CHK;
  }
  

  static public Key read_put_stream(String keyname, InputStream is, byte rf) throws IOException {
    // Main Key
    Key key = Key.make(keyname,rf);
    return read_put_stream(key,is);
  }
  
  // Reads the given stream and creates a value for it, or a list of chunks and
  // an arraylet if the value is too big. 
  // Maybe this should be somehow merged with read_put_file ? 
  static public Key read_put_stream(Key key, InputStream is) throws IOException {
    BufferedInputStream bis = new BufferedInputStream(is,(int)chunk_size()*2);

    byte[] buffer = new byte[(int)chunk_size()*2];
    int sz = 0;
    while (sz!=buffer.length) {
      int i = bis.read(buffer,sz,buffer.length-sz);
      if (i==-1)
        break;
      sz += i;
    }
    if (sz<buffer.length) {
      // it is a single simple value
      Value val = new Value(key,(int)sz);
      System.arraycopy(buffer,0,val.mem(),0,(int)sz);
      UKV.put(key,val);
    } else {
      long offset = 0;
      ValueArray ary = new ValueArray(key,0);
      Key ck = ary.make_chunkkey(offset);
      Value val = new Value(ck,(int)chunk_size());
      System.arraycopy(buffer,0,val.mem(),0,(int)chunk_size());
      DKV.put(ck,val);
      ck = ary.make_chunkkey(offset);
      val = new Value(ck,(int)chunk_size());
      System.arraycopy(buffer,(int)chunk_size(),val.mem(),0,(int)sz);
      DKV.put(ck,val);
      offset += sz;
      while (sz!=0) {
        sz = 0;
        while (sz!=chunk_size()) {
          int i = bis.read(buffer,sz,(int)chunk_size()-sz);
          if (i==-1)
            break;
          sz += i;
        }
        sz = bis.read(buffer,0,(int)chunk_size());
        ck = ary.make_chunkkey(offset);
        val = new Value(ck,(int)sz);
        System.arraycopy(buffer,0,val.mem(),0,(int)sz);
        DKV.put(ck,val);
        offset += sz;
        if (sz!=chunk_size())
          break;
      }
      UDP.set8(ary.mem(),2,offset);
      DKV.put(key,ary);
    }
    bis.close();
    return key;
  }  

  // Read a (possibly VERY large file) and put it in the K/V store and return a
  // Value for it.  Files larger than 2Meg are broken into arraylets of 1Meg each.
  static public Key read_put_file(String keyname, FileInputStream fis, byte rf) throws IOException {
    long sz = fis.getChannel().size();
    DataInputStream dis = new DataInputStream(fis);
    // Main Key
    Key key = Key.make(keyname,rf);
    // Files of modest size, from 0 to <2Megs we represent as a single Value.
    // Larger files are broken up in 1Meg chunks
    int chunks = chunks(sz);    // Divide by 1Meg into chunks, rounding up
    if( chunks < 2 ) {          // Not enough chunks, so use a single Value
      Value val = new Value(key,(int)sz);      
      dis.readFully(val.mem());      
      UKV.put(key,val);         // Insert in distributed store
      return key;
    }

    // Must be a large file; break it up!
    // Main Value: contains a UUID to avoid collions with other big arrays
    // mapping to the same key.
    ValueArray ary = new ValueArray(key,sz);

    // Begin to read & build chunks.
    long off = 0;
    for( int i=0; i<chunks; i++ ) {
      // All-the-rest for the last chunk, or 1Meg for other chunks
      long szl = (i==chunks-1) ? (sz-off) : (1<<LOG_CHK);
      int sz2 = (int)szl;       // Truncate
      assert sz2 == szl;        // No int/long truncation
      
      Key ckey = ary.make_chunkkey(off);
      Value val = new Value(ckey,sz2);
      dis.readFully(val.mem());
      // Twiddle the key name for this chunk
      
      DKV.put(ckey,val);         // Insert in distributed store

      off += szl;               // Advance the cursor
    }
    assert off == sz;           // Got them all

    // Now insert the main Key
    UKV.put(key,ary);         // Insert in distributed store    
    return key;
  }
}
