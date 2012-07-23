package water;
import java.io.*;
import java.util.Arrays;
import java.lang.reflect.Field;

import sun.misc.Unsafe;
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
  private volatile byte[] _mem; 
  public final byte[] mem() { return _mem; }

  public final byte [] allocateMem(int size) {
    byte [] oldMem = _mem;         
    if( (oldMem == null) || (oldMem.length < size)) {
      byte [] newMem = MemoryManager.allocateMemory(size);
      return CAS_mem_if_larger(newMem);
    } else {
      return oldMem;
    }     
  }
  
  // --- Bits to allow atomic update of the Value mem field
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
  private static final long _mem_offset;
  static {                      // <clinit>
    Field f = null;
    try { 
      f = Value.class.getDeclaredField("_mem"); 
    } catch( java.lang.NoSuchFieldException e ) { System.err.println("Can't happen");
    } 
    _mem_offset = _unsafe.objectFieldOffset(f);
  }

  // Classic Compare-And-Swap of mem field
  final boolean CAS_mem( byte[] old, byte[] nnn ) {
    if( old == nnn ) return true;
    if(_unsafe.compareAndSwapObject(this, _mem_offset, old, nnn )) {
      MemoryManager.freeMemory(old);
      return true;
    } else
      return false;
  }
  // Convenience for tracking in-use memory
  final public void free_mem( ) { CAS_mem(_mem,null); }
  final byte[] CAS_mem_if_larger( byte[] nnn ) {
    while( true ) {
      byte[] b = _mem;          // Read it again
      if( b != null && b.length >= nnn.length ) { 
        MemoryManager.freeMemory(nnn); // Free 'nnn' and keep '_mem'
        return b;
      }
      if( CAS_mem(b,nnn) )
        return nnn;
    }
  }

  // The FAST path get-byte-array - final method for speed
  public final byte[] get( int len ) {
    if( len > _max ) len = _max;
    if( _mem != null && len <= _mem.length ) return _mem;
    if( _max == 0 ) return _mem;
    assert (_key!=null) && _key.desired()>0 && is_persisted();  // Should already be on disk!
    return CAS_mem_if_larger(load_persist(len));
  }
  
  public final byte[] get() { return get(Integer.MAX_VALUE); }
  
  // ---
  // time of last access to this value 
  long _lastAccessedTime = System.currentTimeMillis();
  public final void touch() {_lastAccessedTime = System.currentTimeMillis();}


  // ---
  // A Value is persisted. The Key is used to define the filename.
  public final Key _key;

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
  // across Value subclasses and is used to de-serialize Values
  /* because I do not know where else to put them, I am putting all types of the
   * values as a list here. 
   * 
   * I - ICE stored value (Level DB)
   * F - local file stored value  (old ice)
   * A - Arraylet
   * 3 - Amazon S3 value.
   * H - Hadoop backed file (Phase I - existing hadoop installation)
   * h - hadoop backed arraylet 
   * C - code on ICE
   * B - HDFS block
   * N - HDFS iNode
   * * - Internal value (no persistence at all)
   */
  public static final byte ICE = (byte)'I';
  public static final byte ARRAYLET = (byte)'A';
  public static final byte CODE = (byte)'C';
  
  public byte type() { return ICE;  }

  protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append(name_persist());
    sb.append(is_persisted() ? "." : "!");
    return false;
  }

  // ---
  // Value persistency information.
  //
  // Each value has its persistence info which packs both the persistence
  // backend index that can be translated to the persistence backend singleton
  // and the persistence state, which determines the state of the persistence
  // of the value.
  public byte _persistenceInfo;
  final boolean CAS_persist( int old, int nnn ) {
    // TODO: MAKE ME ATOMIC PLEASE
    int tmp = _persistenceInfo&0xFF;
    if( tmp!=old ) return false;
    _persistenceInfo = (byte)nnn;
    return true;
  }

  // Asks  if persistence goal is to either persist (or to remove).
  // True  if the last call was to start(),
  // False if the last call was to remove().
  boolean is_goal_persist() {    return Persistence.p(this).is_goal(this); }
  // Asks if persistence is completed (of either storing or deletion)
  public boolean is_persisted() {return Persistence.p(this).is(this); }
  // Pretty name
  public String name_persist() { return Persistence.p(this).name(this); }
  // Start this Value persisting.  Ok to call repeatedly, or if the value is
  // already persisted.  Depending on how busy the disk is, and how big the
  // Value is, it might be a long time before the value is persisted.
  void start_persist() {                Persistence.p(this).store(this); }
  // Remove any trace of this value from the persistence layer.  Called right
  // after the Value is itself deleted.  Depending on how busy the disk is, and
  // how big the Value is, it might be a long time before the disk space is
  // returned.
  void remove_persist() {               Persistence.p(this).delete(this); }
  // Load more of this Value from the persistence layer
  byte[] load_persist(int len) { return Persistence.p(this).load(this,len); }

  // ---
  // Larger values are chunked into arraylets.  This is the number of chunks:
  // by default the Value is its own single chunk.
  public int chunks() { return 1; }
  public long chunk_offset( int chunknum ) { return chunknum << ValueArray.LOG_CHK; }
  public Key chunk_get( int chunknum ) {
    if( chunknum != 0 ) throw new ArrayIndexOutOfBoundsException(chunknum);
    return _key;                // Self-key
  }

  
  // --------------------------------------------------------------------------
  // Set just the initial fields
  public Value(int max, int length, Key k, int mode) {
    if( length > 0 )
      _mem = MemoryManager.allocateMemory(length);
    _max = max;
    _key = k;
    _persistenceInfo = (byte)mode; // default persistence and mode
  }
  
  public Value(Key key, int max) {
    this(max,max,key,PersistIce.INIT);
  }

  public Value( Key key, String s ) { 
    this(key,s.getBytes().length);
    byte [] sbytes = s.getBytes();
    byte [] mem = mem();
    for(int i = 0; i < sbytes.length; ++i){
      mem[i] = sbytes[i];
    }
  }
  

  // Returns true if the entire value is resident in memory.
  final boolean is_mem_local() { return _mem != null && _mem.length == _max; }

  // Check that these are the same values... but one might be a prefix _mem of
  // the other.  This is not an absolute test: the Values might differ even if
  // this reports 'true'.  However, if it reports 'false' then the Values
  // definitely different.  Does no disk i/o.
  boolean false_ifunequals( Value val ) {
    if( _max != val._max ) return false;
    if( !is_same_key(val) ) return false;
    // If we have any cached bits, they need to be equal.
    if( _mem!=null && val._mem!=null &&
        !equal_buf_chk(val._mem,0,_mem,0,Math.min(val._mem.length,_mem.length)) )
      return false;
    return true;
  }
  // If this reports 'true' then the Values are definitely Equals.
  // If this reports 'false' then the Values might still be equals.
  boolean true_ifequals( Value val ) {
    if( !false_ifunequals(val) ) return false; // Definitely Not Equals
    if( val._mem != null && val._mem.length == _max && 
            _mem != null &&     _mem.length == _max )
      return true;              // Definitely Equals
    return false;               // Possibly equals but reporting not-equals
  }
  // True equals test.  May require disk I/O
  boolean equals( Value val ) {
    if( this == val ) return true;
    if( !is_same_key(val) ) return false;
    if( _max != val._max ) return false;
    return Arrays.equals(val.get(),get());
  }

  // --------------------------------------------------------------------------
  // Serialized format length 1+1+4+4+len bytes
  final int wire_len(int len) {
    return 1/*value-type*/+1/*persist info*/+4/*len*/+4/*max*/+(len>0?len:0);
  }

  // Write up to len bytes to the packet
  final int write( byte[] buf, int off, int len ) {
    assert (len <= _max) || (_max<0);
    buf[off++] = type();        // Value type
    buf[off++] = _persistenceInfo;
    off += UDP.set4(buf,off,len);
    off += UDP.set4(buf,off,_max);
    if(len > 0 ) {              // Deleted keys have -1 len/max
      get(len);                 // Force in-memory from disk
      System.arraycopy(_mem,0,buf,off,len);
      off += len;
    }
    assert off < MultiCast.MTU;
    return off;
  } 

  // Write up to len bytes of Value to the Stream
  final void write( DataOutputStream dos, int len ) throws IOException {
    if( len > _max ) len = _max;
    dos.writeByte(type());      // Value type
    dos.writeByte(_persistenceInfo);      // Value type
    dos.writeInt(len);
    dos.writeInt(_max);
    if( len > 0 ) {             // Deleted keys have -1 len/max
      get(len);                 // Force in-memory from disk
      dos.write(_mem,0,len);    // Sub-class specific data
    }
  }

  static Value construct(int max, int len, Key key, byte p, byte type) {
    switch (type) {
    case 'A': return new ValueArray(max,len,key,p);
    case 'C': return new ValueCode (max,len,key,p);
    case 'I': return new Value     (max,len,key,p);
    default:
      throw new Error("Unable to construct value of type "+(char)(type)+"(0x"+Integer.toHexString(0xff & type)+" (key "+key.toString()+")");
    }
  }
  
  // Read 4+4+len+vc value bytes from the the UDP packet and into a new Value.
  static Value read( byte[] buf, int off, Key key ) {
    byte type = buf[off++];
    byte p = buf[off++];
    int len = UDP.get4(buf,off); off += 4;
    int max = UDP.get4(buf,off); off += 4;
    Value val = construct(max,len,key,p,type);
    byte [] mem = val.mem();
    System.arraycopy(buf,off,mem,0,len);
    return val;
  }
  static Value read( DataInputStream dis, Key key ) throws IOException {
    byte type = dis.readByte();
    byte p = dis.readByte();
    int len = dis.readInt();
    int max = dis.readInt();
    Value val = construct(max,len,key,p,type);
    val._persistenceInfo = p;
    if( len > 0 )
      dis.readFully(val.mem());
    return val;
  }

  // This can only *extend* a Value, and is used when we have partially cached
  // Value and are caching more of it.
  Value extend( Value val ) {
    throw new Error("unimplemented extend");
    //assert _max==val._max;
    //if( !is_goal_persist() ) return this; // deleted-key is already loaded
    //byte[] mem = _mem;
    //while( true ) {
    //  int oldlen = mem==null ? 0 : mem.length;
    //  assert equal_buf_chk(val._mem,0,mem,0,Math.min(val._mem.length,oldlen));
    //  if( val._mem.length <= oldlen ) return this; // Already loaded elsewhere
    //  // Attempt atomic update of _mem
    //  if( CAS_mem(mem,val._mem) )
    //    return this;
    //  mem = _mem;
    //}
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
    if( !is_goal_persist() ) return sb.append("[deleted]").toString();
    // Ensure at least 'len' bytes are memory-local
    get(len);
    sb.append("[");
    if( _mem == null ) return sb.append(_max).append(_max==0?"]":"] ioerror").toString();
    sb.append(_mem.length).append("/").append(_max).append("]=");
    // Check for 'string-like' bytes in the 1st len bytes
    for( int i=0; i<len; i++ ) {
      byte b = _mem[i];
      sb.append(b < 32 ? '.' : (char)b);
    }
    if( len < _max ) sb.append("...");
    return sb.toString();
  }
  
  /** Returns a stream that can read the value. 
   * @return 
   */
  public InputStream openStream() {
    if (chunks()==1) {
      return new ByteArrayInputStream(get());
    } else {
      return new ArrayletInputStream(this);
    }    
  }
  
  public long length() { return _max<0?0:_max; }
}


class ArrayletInputStream extends InputStream {

  // arraylet value
  private Value _arraylet;
  // memory for the current chunk
  private byte[] _mem;
  // index of the current chunk
  private int _chunkIndex;
  // offset in the memory of the current chunk
  private int _offset;
  
  public ArrayletInputStream(Value v) {
    assert (v.chunks()>1);
    _arraylet = v;
    _mem = null;
    _chunkIndex = -1;
    _offset = 0;
  }
  
  @Override public int available() {
    checkAndSetChunk();
    return _mem == null ? -1 : _mem.length-_offset;
  }

  
  private void checkAndSetChunk() {
    if (((_mem == null) || (_offset>=_mem.length)) && (_chunkIndex < _arraylet.chunks()-1)) {
      ++_chunkIndex;
      _mem = DKV.get(_arraylet.chunk_get(_chunkIndex)).get();
      _offset = 0;
    }
  }
  
  @Override public void close() {
    // just get rid of the memory
    _mem = null;
  }
  
  @Override public int read() throws IOException {
    checkAndSetChunk();
    if (_mem == null)
      return -1;
    return _mem[_offset++];
  }
  
  @Override public int read(byte[] b, int off,int len) throws IOException {
    int rc = 0;
    while (len>0) {
      checkAndSetChunk();
      if (_mem == null)
        break;
      int cs = Math.min(_mem.length-_offset,len);
      System.arraycopy(_mem,_offset,b,off,cs);
      rc += cs;
      len -= cs;
    }
    return rc == 0 ? -1 : rc;
  }
  
}
