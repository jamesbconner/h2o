package water;
import java.io.*;
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
  
  /** Value persistency information.
   * 
   * Each value has its persistence info which packs both the persistence
   * backend index that can be translated to the persistence backend singleton
   * and the persistence state, which determines the state of the persistence
   * of the value. 
   */
  private byte _persistenceInfo;

  /** Value is not persisted and its persistence has not been started yet.
   * 
   * The value will eventually be persisted. 
   */
  public static final int NOT_STARTED = 0;
  /** Value is in the middle of persisting. 
   * 
   * If successful it will automatically change to PERSISTED, if not, it flips
   * back to IN_PROGRESS.
   */
  public static final int IN_PROGRESS = 1;
  /** The value is persisted. 
   * 
   * Everything is ok with this value.
   */
  public static final int PERSISTED = 2;
  
  /** Indicates that the value should not be persisted and must always remain
   *  in memory. 
   * 
   * Be careful with values of this type as they can never be deleted by the
   * memory manager and will occupy memory until they are removed from the KV
   * store.
   */
  public static final int DO_NOT_PERSIST = 3;
  
  /** Never persisted cache value. 
   * 
   * Similar to DO_NOT_PERSIST, this value is never persisted, but unlike it,
   * this value can be deleted by the memory manager, in which case the whole
   * value is removed from the KV store. 
   */
  public static final int CACHE = 4;
  
  
  public static String persistenceStateToString(int state) {
    switch (state) {
    case NOT_STARTED:   return "NOT_STARTED";
    case IN_PROGRESS:   return "IN_PROGRESS";
    case PERSISTED:     return "PERSISTED";
    case DO_NOT_PERSIST:return "DO_NOT_PERSIST";
    case CACHE:         return "CACHE";
    default:            return "INVALID ("+state+")";
    }
  }
  
  // Decodes persistence state from given byte
  private static int persistenceState(byte from) {
    return byteToUnsigned(from) & 0x07;
  }
  
  // Encodes the persistence state to given byte
  private static byte setPersistenceState(byte from, int value) {
    assert (value>=0) && (value<8);
    return (byte) ((byteToUnsigned(from) & 0xf8) | value);
  }
  
  // Decodes the persistence backend from given byte
  private static Persistence persistenceBackend(byte from) {
    return Persistence.getPersistence(byteToUnsigned(from) >> 3);
  }
  
  // Encodes the persistence backend to the given byte
  private static byte setPersistenceBackend(byte from, Persistence value) {
    assert (value.type().id>=0) && (value.type().id<32);
    return (byte) ((byteToUnsigned(from) & 0x07) | (value.type().id << 3));
  }
  
  // Converts the byte to an unsigned bit representation
  private static int byteToUnsigned(byte what) {
    return what & 0xff;
  }
  
  /** Returns the persistence state of the value. */
  public int persistenceState() {
    return persistenceState(_persistenceInfo);    
  }
  
  /** Sets the persistence state of the value. */
  protected void setPersistenceState(int value) {
    _persistenceInfo = setPersistenceState(_persistenceInfo,value);
  }
  
  /** Returns the persistence backend of the value. */
  public Persistence persistenceBackend() {
    return persistenceBackend(_persistenceInfo);
  }
  
  /** Sets the persistence backend of the value. */
  public void setPersistenceBackend(Persistence value) {
    _persistenceInfo = setPersistenceBackend(_persistenceInfo,value);
  }

  /** Creates the value from the wire. 
   * 
   * Must be rewritten by each subclass to return the correct type. 
   * 
   * @param max
   * @param len
   * @param vc
   * @param vcl
   * @param key
   * @return 
   */
  static Value make_wire(int max, int len, Key key ) {
    return new Value(max,len,key,NOT_STARTED);
  }
  
  /// Loads the value with itself as being the sentinel.
  protected Value load(Key k) {
    Persistence p = persistenceBackend();
    return p.load(k,this);
  } 

  // Values are wads of bits; known small enough to 'chunk' politely on disk,
  // or fit in a Java heap (larger Values are built via arraylets) but (much)
  // larger than a UDP packet.  Values can point to either the disk or ram
  // version or both.  There's no caching smarts, nor compression nor de-dup
  // smarts.  This is just a local placeholder for some user bits being held at
  // this local Node.

  public final int _max; // Max length of Value bytes
  // Returns true, if the value is or should be deleted.
  public final boolean is_deleted() { return (this instanceof ValueDeleted); }
  public final boolean is_persisted() { return (persistenceState()==PERSISTED);  }
  public final boolean is_sentinel() { return this instanceof ValueSentinel && !(this instanceof ValueDeleted); }

  public final void touch() {_lastAccessedTime = System.currentTimeMillis();}

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
    assert ((_key!=null) && (_key.desired()==0)) || is_local_persist();  // Should already be on disk!
    return CAS_mem_if_larger(persistenceBackend().get(_key,this,len));
  }
  
  public final byte[] get() { return get(Integer.MAX_VALUE); }
  

  // Larger values are chunked into arraylets.  This is the number of chunks:
  // by default the Value is its own single chunk.
  public int chunks() { return 1; }
  public long chunk_offset( int chunknum ) { return chunknum << ValueArray.LOG_CHK; }
  public Key chunk_get( int chunknum ) {
    if( chunknum != 0 ) throw new ArrayIndexOutOfBoundsException(chunknum);
    return _key;                // Self-key
  }

  // A Value is persisted. The Key is used to define the filename.
  protected Key _key;

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
  public byte type() {
    return 'I';
  }

  
  public static final byte ICE = (byte)'I';
  public static final byte ARRAYLET = (byte)'A';
  public static final byte CODE = (byte)'C';

  
  protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append(persistenceBackend().name());
    sb.append(is_local_persist() ? "." : "!");
    return false;
  }

  
  // Makes a "deleted" Value of the correct persistence type.  When this Value
  // is persisted, it removes any trace from the persistence layer (e.g. disk).
  // Its persistence state is changed to DELETE_PENDING, all of its data is
  // freed
  // @param key
  public Value makeDeleted(Key key) {
    Persistence p = persistenceBackend();
    return ValueDeleted.create(p.sentinelData(key,this),key,persistenceBackend(),type());
  }
  
  // time of last access to this value 
  long _lastAccessedTime = System.currentTimeMillis();
  
  /** Marks the value as cached value that can be deleted. Sets its persistence
   * state to CACHE. 
   */
  public void markAsCache() {
    setPersistenceState(CACHE);
  }
  
  /** Marks the value as not persistent value that cannot be deleted. Sets its
   * persistence state to DO_NOT_PERSIST. 
   */
  public void markAsNotPersistent() {
    setPersistenceState(DO_NOT_PERSIST);
  }
  
  // ---------------------------------------------------------------------------
  // Is this value "locally" persisted - persisted as far as this Nodes'
  // responsiblity goes?  Typically this means its on the local disk or similar
  // hardware-local storage.  Asking this question is also used to "wake up"
  // the persistence layer for this Value, and may begin persistence (even as
  // 'false' is returned because persistence is not complete).
  protected boolean is_local_persist(Value oldValue) {
    // It makes no sense to run is_local_persist on values with no key. 
    if (_key == null)
      return true;
    // if the persistence state demands the value does not persist, pretend that
    // it is persisted. 
    if ((persistenceState()==DO_NOT_PERSIST) || (persistenceState()==CACHE))
      return true;
    if ( is_persisted()) return true; // Fast-path cutout
    // Lock, and flip to IN_PROGRESS
    synchronized(this) {
      if( persistenceState() != NOT_STARTED ) return false;
      setPersistenceState(IN_PROGRESS);
    }
    // Unlock, and go the underlying persistence layer
    boolean res = false;
    if (this instanceof ValueDeleted) {
      res = persistenceBackend().delete(_key,this);
    } else {
      if (oldValue!=null)
        oldValue.persistenceBackend().delete(_key,oldValue);
      res = persistenceBackend().store(_key,this);
    }
    // Persistence layer returns, but only returns true if failed.  So this is
    // design requires I/O to be *blocking*, because there's no 3rd return
    // value meaning "still working in the background"
    if (!res) {
      synchronized (this) {
        assert persistenceState() == IN_PROGRESS; // assert only makes sense in a blocking i/o design
        setPersistenceState(NOT_STARTED);
      }
    } else {
      synchronized (this) {
        assert persistenceState() == IN_PROGRESS; // assert only makes sense in a blocking i/o design
        setPersistenceState(PERSISTED);
        // Notify MM this guy is persisted, and thus can have memory freed
        MemoryManager.notifyValuePersisted(this);
      }      
    }
    return res;
  }
  
  // Same as the above, except the Value may have changed basic types recently
  // which changes the filename on disk.  We need the old type so we can nuke
  // the old file under the old name.
  protected boolean is_local_persist() { return is_local_persist(null); }
  
  // --------------------------------------------------------------------------
  // Test if this Value is out-of-order with the given Value.  Only useful if a
  // Value is updated.  If these are all local writes, then the local Node
  // defines the ordering.  If these are remote writes, then TaskGetKeys are
  // used to prevent a late-arriving 1st TGK from overwriting a quick-arriving
  // 2nd TGK (from the same remote writer).  If we have racing unrelated remote
  // writers, then always the most recent arriving write wins.
  public boolean happens_before( Value val ) {
    throw new Error("unimplemented");
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
    this(max,max,key,NOT_STARTED);
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
  // definitely different.
  boolean equals_val( Value val ) {
    if( _max != val._max ) return false;
    if( !is_same_key(val) ) return false;
    // If we have any cached bits, they need to be equal.
    if( _mem!=null && val._mem!=null &&
        !equal_buf_chk(val._mem,0,_mem,0,Math.min(val._mem.length,_mem.length)) )
      return false;
    return true;
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
    Value result = null;
    if (max<0) {
      assert (len == 0);
      if ((max & ValueDeleted.MAX_MASK) == ValueDeleted.MAX_MASK)
        result = new ValueDeleted(max,len,key,persistenceState(p));
      else if ((max & ValueSentinel.MAX_MASK) == ValueSentinel.MAX_MASK)
        result = new ValueSentinel(max,len,key,persistenceState(p));
      else
        throw new Error("Unable to construct value with negative max ("+max+") that does not correspond to any sentinel type (key "+key.toString()+")");
    } else {
      switch (type) {
      case 'A': result = ValueArray.make_wire(max,len,key); break;
      case 'C': result = ValueCode .make_wire(max,len,key); break;
      case 'I': result = Value     .make_wire(max,len,key); break;
      default:
        throw new Error("Unable to construct value of type "+(char)(type)+"(0x"+Integer.toHexString(0xff & type)+" (key "+key.toString()+")");
      }
    }
    result.setPersistenceBackend(persistenceBackend(p));
    return result;
  }
  
  // Read 4+4+len+vc value bytes from the the UDP packet and into a new Value.
  static Value read( byte[] buf, int off, Key key ) {
    byte type = buf[off++];
    byte p = buf[off++];
    int len = UDP.get4(buf,off); off += 4;
    int max = UDP.get4(buf,off); off += 4;
    System.out.println("val read of "+len+"/"+max);
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
    assert _max==val._max;
    if( is_deleted() ) return this; // deleted-key is already loaded
    byte[] mem = _mem;
    while( true ) {
      int oldlen = mem==null ? 0 : mem.length;
      assert equal_buf_chk(val._mem,0,mem,0,Math.min(val._mem.length,oldlen));
      if( val._mem.length <= oldlen ) return this; // Already loaded elsewhere
      // Attempt atomic update of _mem
      if( CAS_mem(mem,val._mem) )
        return this;
      mem = _mem;
    }
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
    if( is_deleted() ) return sb.append("[deleted]").toString();
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
