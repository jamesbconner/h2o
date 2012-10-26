package water;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.UUID;

import sun.misc.Unsafe;
import water.hdfs.PersistHdfs;
import water.nbhm.UtilUnsafe;

/**
 * Values
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class Value {

  // ---
  // Values are wads of bits; known small enough to 'chunk' politely on disk,
  // or fit in a Java heap (larger Values are built via arraylets) but (much)
  // larger than a UDP packet.  Values can point to either the disk or ram
  // version or both.  There's no caching smarts, nor compression nor de-dup
  // smarts.  This is just a local placeholder for some user bits being held at
  // this local Node.
  public final int _max; // Max length of Value bytes

  // ---
  // Backend persistence info.  3 bits are reserved for 8 different flavors of
  // backend storage.  1 bit for whether or not the latest _mem field is
  // entirely persisted on the backend storage, or not.  Note that with only 1
  // bit here there is an unclosable datarace: one thread could be trying to
  // change _mem (e.g. to null for deletion) while another is trying to write
  // the existing _mem to disk (for persistence).  This datarace only happens
  // if we have racing deletes of an existing key, along with racing persist
  // attempts.  There are other races that are stopped higher up the stack: we
  // do not attempt to write to disk, unless we have *all* of a Value, so
  // extending _mem (from a remote read) should not conflict with writing _mem
  // to disk.
  //
  // The low 3 bits are final.
  // The on/off disk bit is strictly cleared by the higher layers (e.g. Value.java)
  // and strictly set by the persistence layers (e.g. PersistIce.java).
  public volatile byte _persist; // 3 bits of backend flavor; 1 bit of disk/notdisk
  public final static byte ICE = 1<<0; // ICE: distributed local disks
  public final static byte HDFS= 2<<0; // HDFS: backed by hadoop cluster
  public final static byte S3  = 3<<0; // Amazon S3
  public final static byte NFS = 4<<0; // NFS: Standard file system
  public final static byte BACKEND_MASK = (8-1);
  public final static byte NOTdsk = 0<<3; // latest _mem is persisted or not
  public final static byte ON_dsk = 1<<3;
  final public void clrdsk() { _persist &= ~ON_dsk; } // note: not atomic
  final public void setdsk() { _persist |=  ON_dsk; } // note: not atomic
  final public boolean is_persisted() { return (_persist&ON_dsk)!=0; }

  // ---
  // A byte[] of this Value when cached in DRAM, or NULL if not cached.  Length
  // cached in ram is _mem.length, which might be less than _max.  The actual
  // contents of _mem are considered immutable (Key/Value mappings can be
  // changed by an explicit PUT action) but prefixes of the full value can be
  // cached in shorter _mem arrays.  Later, the prefix might be extended to a
  // longer version, changing the _mem field itself without changing it's "as
  // if immutable" symantics.
  //
  // This field is atomically updated (see CAS_mem below), to prevent racing
  // parallel update threads from setting different _mem arrays representing
  // different lengths of caching.
  protected volatile byte[] _mem;
  public final byte[] mem() { return _mem; }

  // --- Bits to allow atomic update of the Value mem field
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final long _mem_offset;
  static {                      // <clinit>
    Field f1=null;
    try {
      f1 = Value.class.getDeclaredField("_mem");
    } catch( java.lang.NoSuchFieldException e ) { System.err.println("Can't happen");
    }
    _mem_offset = _unsafe.objectFieldOffset(f1);
  }

  // Classic Compare-And-Swap of mem field
  final boolean CAS_mem( byte[] old, byte[] nnn ) {
    if( old == nnn ) return true;
    return _unsafe.compareAndSwapObject(this, _mem_offset, old, nnn );
  }
  // Convenience for tracking in-use memory
  final public void free_mem( ) { CAS_mem(_mem,null); }
  // CAS in a larger byte[], returning the current one when done.
  final byte[] CAS_mem_if_larger( byte[] nnn ) {
    if( nnn == null ) return _mem;
    while( true ) {
      byte[] b = _mem;          // Read it again
      if( b != null && b.length >= nnn.length )
        return b;
      if( CAS_mem(b,nnn) )
        return nnn;
    }
  }

  // The FAST path get-byte-array - final method for speed.
  // Returns a NULL if the Value is deleted already.
  public final byte[] get() { return get(Integer.MAX_VALUE); }
  public final byte[] get( int len ) {
    if( len > _max ) len = _max;
    byte[] mem = _mem;          // Read once!
    if( mem != null && len <= mem.length ) return mem;
    if( mem != null && _max == 0 ) return mem;
    byte[] newmem = (_max==0 ? new byte[0] : load_persist(len));
    return CAS_mem_if_larger(newmem); // CAS in the larger read
  }


  // ---
  // Time of last access to this value.
  long _lastAccessedTime = System.currentTimeMillis();
  public final void touch() {_lastAccessedTime = System.currentTimeMillis();}


  // ---
  // A Value is persisted. The Key is used to define the filename.
  public Key _key;

  // Assertion check that Keys match, for those Values that require an internal
  // Key (usually for disk filename persistence).
  protected boolean is_same_key(Key key) { return (_key==null) || (_key == key); }
  protected boolean is_same_key(water.Value val) {
    return _key== val._key;
  }

  // ---------------------------------------------------------------------------
  // Abstract interface for value subtypes
  // ---------------------------------------------------------------------------
  // A 1-byte ASCII char type-field for Values.  This byte must be unique
  // across Value subclasses and is used to de-serialize Values.
  //
  // V - Normal Value.
  // A - Array Head; types as a ValueArray.
  public static final byte VALUE = (byte)'V';
  public static final byte ARRAY = (byte)'A';

  public byte type() { return VALUE; }

  protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append(name_persist());
    sb.append(is_persisted() ? "." : "!");
    return false;
  }

  // ---
  // Interface for using the persistence layer(s).
  // Store complete Values to disk
  void store_persist() {
    if( is_persisted() ) return;
    switch( _persist&BACKEND_MASK ) {
    case ICE : PersistIce .file_store(this); break;
    case HDFS: PersistHdfs.file_store(this); break;
    case NFS : PersistNFS .file_store(this); break;
    default  : throw H2O.unimpl();
    }
  }
  // Remove dead Values from disk
  void remove_persist() {
    // do not yank memory, as we could have a racing get hold on to this
    //  free_mem();
    if( !is_persisted() ) return; // Never hit disk?
    clrdsk();                   // Not persisted now
    switch( _persist&BACKEND_MASK ) {
    case ICE : PersistIce .file_delete(this); break;
    case HDFS: PersistHdfs.file_delete(this); break;
    case NFS : PersistNFS .file_delete(this); break;
    default  : throw H2O.unimpl();
    }
  }
  // Load some or all of completely persisted Values
  byte[] load_persist(int len) {
    assert is_persisted();
    switch( _persist&BACKEND_MASK ) {
    case ICE : return PersistIce .file_load(this,len);
    case HDFS: return PersistHdfs.file_load(this,len);
    case NFS : return PersistNFS .file_load(this,len);
    default  : throw H2O.unimpl();
    }
  }

  public String name_persist() {
    switch( _persist&BACKEND_MASK ) {
    case ICE : return "ICE";
    case HDFS: return "HDFS";
    case S3  : return "S3";
    case NFS : return "NFS";
    default  : throw H2O.unimpl();
    }
  }

  // Lazily manifest data chunks on demand.  Requires a pre-existing ValueArray.
  // Probably should be moved into HDFS-land, except that the same logic applies
  // to all stores providing large-file access by default including S3.
  public static Value lazy_array_chunk( Key key ) {
    if( key._kb[0] != Key.ARRAYLET_CHUNK ) return null; // Not an arraylet chunk
    Key arykey = Key.make(ValueArray.getArrayKeyBytes(key));
    Value v1 = DKV.get(arykey);
    if( v1 == null ) return null;                  // Nope; not there
    if( !(v1 instanceof ValueArray) ) return null; // Or not a ValueArray
    switch( v1._persist&BACKEND_MASK ) {
    case ICE : if( !key.home() ) return null; // Only do this on the home node for ICE
               return PersistIce .lazy_array_chunk(key);
    case HDFS: return PersistHdfs.lazy_array_chunk(key);
    case NFS : return PersistNFS .lazy_array_chunk(key);
    default  : throw H2O.unimpl();
    }
  }

  // ---
  // Larger values are chunked into arraylets.  This is the number of chunks:
  // by default the Value is its own single chunk.
  public long chunks() { return 1; }
  static public long chunk_offset( long chunknum ) { return chunknum << ValueArray.LOG_CHK; }
  public Key chunk_get( long chunknum ) {
    if( chunknum != 0 ) throw new ArrayIndexOutOfBoundsException(Long.toString(chunknum));
    return _key;                // Self-key
  }

  // --------------------------------------------------------------------------
  // Set just the initial fields
  public Value(int max, int length, Key k, byte be ) {
    _mem = MemoryManager.allocateMemory(length);
    _max = max;
    _key = k;
    // For the ICE backend, assume new values are not-yet-written.
    // For HDFS & NFS backends, assume we from global data and preserve the
    // passed-in persist bits
    byte p = (byte)(be&BACKEND_MASK);
    _persist = (p==ICE) ? p : be;
  }

  public Value(Key key, int max) {
    this(max,max,key,ICE);
  }

  public Value( Key key, String s ) {
    this(key,s.length());
    System.arraycopy(s.getBytes(),0,_mem,0,_mem.length);
  }
  // Memory came from elsewhere
  public Value(Key k, byte[] bits ) {
    _mem = bits;
    _max = bits.length;
    _key = k;
    _persist = ICE;
  }

  // Memory came from elsewhere
  public Value(Key k, byte[] bits, byte mode) {
    _mem = bits;
    _max = bits.length;
    _key = k;
    _persist = mode;
  }

  // Check that these are the same values... but one might be a prefix _mem of
  // the other.  This is not an absolute test: the Values might differ even if
  // this reports 'true'.  However, if it reports 'false' then the Values
  // definitely different.  Does no disk i/o.
  boolean false_ifunequals( Value val ) { return false_ifunequals(val,_mem,val._mem);  }
  boolean false_ifunequals( Value val, byte[] mem1, byte[] mem2 ) {
    if( _max != val._max ) return false;
    if( _key != val._key && !_key.equals(val._key) ) return false;
    // If we have any cached bits, they need to be equal.
    if( mem1!=null && mem2!=null &&
        !equal_buf_chk(mem2,0,mem1,0,Math.min(mem2.length,mem1.length)) )
      return false;
    return true;
  }
  // If this reports 'true' then the Values are definitely Equals.
  // If this reports 'false' then the Values might still be equals.
  boolean true_ifequals( Value val ) {
    byte[] mem1 = _mem;
    byte[] mem2 = val._mem;
    if( !false_ifunequals(val,mem1,mem2) ) return false; // Definitely Not Equals
    if( mem2 != null && mem2.length == _max &&
        mem1 != null && mem1.length == _max )
      return true;              // Definitely Equals
    return false;               // Possibly equals but reporting not-equals
  }
  // True equals test.  May require disk I/O
  boolean equals( Value val ) {
    if( this == val ) return true;
    if( _key != val._key && !_key.equals(val._key) ) return false;
    if( _max != val._max ) return false;
    return Arrays.equals(val.get(),get());
  }

  // Expand a KEY_OF_KEYS into an array of keys
  public Key[] flatten() {
    assert _key._kb[0] == Key.KEY_OF_KEYS;
    byte[] buf = get();
    int off = 0;
    int klen = UDP.get4(buf,(off+=4)-4);
    Key[] keys = new Key[klen];
    for( int i=0; i<klen; i++ ) {
      Key k = keys[i] = Key.read(buf,off);
      off += k.wire_len();
    }
    return keys;
  }

  // --------------------------------------------------------------------------
  // Serialized format length 1+1+4+4+len bytes
  final int wire_len(int len) {
    return 1/*value-type*/+1/*persist info*/+4/*len*/+4/*max*/+(len>0?len:0);
  }

  public final void write( Stream s, int len ) { write(s, len, len > 0 ? get(len) : null); }
  public final void write( Stream s, int len, byte[] vbuf ) {
    s.set1(type());
    s.set1(_persist);
    s.set4(len);
    s.set4(_max);
    if( len > 0 ) s.setBytes(vbuf, len);
  }

  // Write up to len bytes of Value to the Stream
  final void write( DataOutputStream dos, int len ) throws IOException {
    write(dos,len,(len > 0) ? get(len):null);
  }
  final void write( DataOutputStream dos, int len, byte[] vbuf ) throws IOException {
    if( len > _max ) len = _max;
    dos.writeByte(type());      // Value type
    dos.writeByte(_persist);    // Value type
    dos.writeInt(len);
    dos.writeInt(_max);
    if( len > 0 )                // Deleted keys have -1 len/max
      dos.write(vbuf,0,len); // Sub-class specific data
  }

  static Value construct(int max, int len, Key key, byte p, byte type) {
    switch (type) {
    case ARRAY: return new ValueArray(max,len,key,p);
    case VALUE: return new Value     (max,len,key,p);
    default:
      throw new Error("Unable to construct value of type "+(char)(type)+"(0x"+Integer.toHexString(0xff & type)+" (key "+key.toString()+")");
    }
  }

  // Read 1+1+4+4+len+vc value bytes from the the UDP packet and into a new Value.
  static Value read( Stream s, Key key ) {
    byte type = s.get1();
    if( type == 0 ) return null;  // Deleted sentinel
    byte p  = s.get1();
    int len = s.get4();
    int max = s.get4();
    Value val = construct(max, len, key, p, type);
    if( len > 0 ) s.getBytes(val.mem(), len);
    return val;
  }

  static Value read( DataInputStream dis, Key key ) throws IOException {
    byte type = dis.readByte();
    byte p  = dis.readByte();
    int len = dis.readInt();
    int max = dis.readInt();
    Value val = construct(max,len,key,p,type);
    if( len > 0 )
      dis.readFully(val.mem());
    return val;
  }

  static boolean equal_buf_chk( byte[] b1, int off1, byte[] b2, int off2, int len ) {
    for( int i=0; i<len; i++ )
      if( b1[i+off1] != b2[i+off2] )
        return false;
    return true;
  }

  // Convert the first len bytes of Value to a pretty-printable String.
  // Try to escape all HTML tags.
  public final String getString( int len ) {
    if( len > _max ) len = _max;
    StringBuilder sb = new StringBuilder(len<0?0:len);
    // Sub-class preliminaries
    if( getString_impl(len,sb) ) return sb.toString();
    // Ensure at least 'len' bytes are memory-local
    byte[] mem = get(len);
    sb.append("[");
    if( mem == null ) return sb.append(_max).append(_max==0?"]":"] ioerror").toString();
    sb.append(mem.length).append("/").append(_max).append("]=");
    for( int i=0; i<len; i++ ) {
      byte b = mem[i];
      if( b=='\r' ) {           // CR?
        if( i+1<len && mem[i+1]=='\n' )
          i++;                  // Skip a trailing LF from a CR-LF pair
        b = '\n';               // Swap CR and CR-LF for plain LF
      }
      if( b >= 32 || b == '\n' ) sb.append((char)b); // Standard ascii, let flow thru
    }
    if( len < _max ) sb.append("...");
    return sb.toString();
  }

  /** Returns a stream that can read the value.
   * @return
   */
  public InputStream openStream() throws IOException {
    return (chunks() <= 1)
      ? new ByteArrayInputStream(DKV.get(_key).get())
      : new ArrayletInputStream(this);
  }

  public long length() { return _max<0?0:_max; }


  public boolean onHDFS(){
    return (_persist & BACKEND_MASK) == HDFS;
  }
  // atomicaly set the backend to hdfs and persist state to not persisted
  // and than remove the old stored value (if any)
  //
  public void switch2HdfsBackend(boolean persisted){
    byte oldPersist = _persist;
    if(persisted)_persist = HDFS|ON_dsk; else _persist = HDFS;
    if((oldPersist & ON_dsk) > 0)
      switch( oldPersist&BACKEND_MASK ) {
      case ICE : PersistIce .file_delete(this); break;
      case HDFS: assert(false); PersistHdfs.file_delete(this); break;
      case NFS : PersistNFS .file_delete(this); break;
      default  : throw H2O.unimpl();
      }
    _key.invalidate_remote_caches();
  }

}


class ArrayletInputStream extends InputStream {
  // arraylet value
  private final ValueArray _arraylet;
  // memory for the current chunk
  private byte[] _mem;
  // index of the current chunk
  private long _chunkIndex;
  // offset in the memory of the current chunk
  private int _offset;

  public ArrayletInputStream(Value v) throws IOException {
    _arraylet = (ValueArray)v;
    _mem = _arraylet.get(_chunkIndex++).get();
  }

  @Override public int available() throws IOException {
    int availableBytes = _mem.length-_offset;
    // Prevent stream close if we are at the end of actual chunk but there is still chunks to read
    // and load the next chunk.
    if (availableBytes == 0 && _chunkIndex < _arraylet.chunks()) {
      _mem    = _arraylet.get(_chunkIndex++).get();
      _offset = 0;
      availableBytes = _mem.length;
    }
    return availableBytes;
  }

  @Override public void close() {
    _chunkIndex = _arraylet.chunks();
    _mem = new byte[0];
    _offset = _mem.length;
  }

  @Override public int read() throws IOException {
    if( available() == 0 ) {    // None available?
      return -1;
    }
    return _mem[_offset++] & 0xFF;
  }

  @Override public int read(byte[] b, int off, int len) throws IOException {
    int rc = 0;  // number of bytes read
    while( len>0 ) {
      int cs = Math.min(available(),len);
      System.arraycopy(_mem,_offset,b,off,cs);
      rc      += cs;
      len     -= cs;
      off     += cs;
      _offset += cs;
      if ( len<=0 ) break;
      if ( available() == 0) {
        if( _chunkIndex >= _arraylet.chunks() ) break;
      }
    }
    return rc == 0 ? -1 : rc;
  }

}
