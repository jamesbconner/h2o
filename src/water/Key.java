package water;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Keys
 *
 * This class defines:
 * - A Key's bytes (name) & hash
 * - Known Disk & memory replicas.
 * - A cache of somewhat expensive to compute stuff related to the current
 *   Cloud, plus a byte of the desired replication factor.
 * 
 * Keys are expected to be a high-count item, hence the care about size.
 *
 * Keys are *interned* in the local K/V store, a non-blocking hash set and are
 * kept pointer equivalent (via the interning) for cheap compares.  The
 * interning only happens after a successful insert in the local H2O.STORE via
 * H2O.put_if_later.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public final class Key implements Comparable {

  // The Key!!!
  // Limited to 512 random bytes - to fit better in UDP packets.
  public static final int KEY_LENGTH = 512;
  public final byte[] _kb;      // Key bytes, wire-line protocol
  final int _hash;              // Hash on key alone (and not value)
  
  // The user keys must be ASCII, so the values 0..31 are reserved for system
  // keys. When you create a system key, please do add its number to this list

  public static final byte ARRAYLET_CHUNK = 0;

  public static final byte DEV_JAR = 1;
  
  public static final byte HDFS_INTERNAL_BLOCK = 10;
  
  public static final byte HDFS_INODE = 11;
  
  public static final byte HDFS_BLOCK_INFO = 12;
  
  public static final byte HDFS_BLOCK_SHADOW = 13;
  
  public static final byte DFJ_INTERNAL_USER = 14;
  
  public static final byte USER_KEY = 32;
  
  
  
  // Known-achieved replication factor.  This is a cache of the first 8 Nodes
  // that have "checked in" with disk completion.  Monotonically increases over
  // time; limit of 8 replicas.  Tossed out on an update.  Implemented as 8 bytes
  // of dense integer indices, H2ONode._unique_idx.  This can be racily set.
  private long _disk_replicas; // Replicas known to be in proper Nodes
  void clr_disk_replicas() { _disk_replicas=0; }
  // Same thing for memory-only replicas.
  private long _mem_replicas; // Replicas known to be in proper Nodes
  void clr_mem_replicas() { _mem_replicas=0; }

  public static byte MAXREPLICATION = 127;

  // 64 bits of Cloud-specific cached stuff.  It is changed atomically by any
  // thread that visits it and has the wrong Cloud.  It has to be read *in the
  // context of a specific Cloud*, since a re-read may be for another Cloud.
  private volatile long _cache;
  private static final AtomicLongFieldUpdater<Key> _cacheUpdater =
    AtomicLongFieldUpdater.newUpdater(Key.class, "_cache");

  // Accessors and updaters for the Cloud-specific cached stuff.
  // The Cloud index, a byte uniquely identifying the last 256 Clouds.  It
  // changes atomically with the _cache word, so we can tell which Cloud this
  // data is a cache of.
  private static int cloud( long cache ) { return (int)(cache>>> 0)&0x00FF; }
  // Shortcut node index for Home replica#0.  This replica is responsible for
  // breaking ties on writes.  'char' because I want an unsigned 16bit thing,
  // limit of 65534 Cloud members.  -1 is reserved for a bare-key
  private static int home ( long cache ) { return (int)(cache>>> 8)&0xFFFF; }
  // Our replica #, or -1 if we're not one of the first 127 replicas.  This
  // value is found using the Cloud distribution function and changes for a
  // changed Cloud.
  private static int replica(long cache) { return (byte)(cache>>>24)&0x00FF; }
  // Desired replication factor.  Can be zero for temp keys.  Not allowed to
  // later, because it messes with e.g. meta-data on disk.
  private static int desired(long cache) { return (int)(cache>>>32)&0x00FF; }

  private static int pad3 ( long cache ) { return (int)(cache>>>40)&0xFFFFFF; }

  private static long build_cache( int cidx, int home, int replica, int desired ) {
    return                      // Build the new cache word
        ((long)(cidx   &0xFF)<< 0) |
        ((long)(home &0xFFFF)<< 8) |
        ((long)(replica&0xFF)<<24) |
        ((long)(desired&0xFF)<<32) |
        ((long)(0           )<<40);
  }

  public int home   ( H2O cloud ) { return home   (cloud_info(cloud)); }
  public int replica( H2O cloud ) { return replica(cloud_info(cloud)); }
  public int desired(           ) { return desired(_cache); }
  public boolean home()           { return home(H2O.CLOUD)==H2O.CLOUD.nidx(H2O.SELF); }
  
  // Update the cache, but only to strictly newer Clouds
  private boolean set_cache( long cache ) {
    while( true ) {                   // Spin till get it
      long old = _cache;              // Read once at the start
      if( !H2O.larger(cloud(cache),cloud(old)) ) // Rolling backwards?
        // Attempt to set for an older Cloud.  Blow out with a failure; caller
        // should retry on a new Cloud.
        return false;
      assert cloud(cache) != cloud(old) || cache == old;
      if( old == cache ) return true; // Fast-path cutout
      if( _cacheUpdater.compareAndSet(this,old,cache) ) return true;
      // Can fail if the cache is really old, and just got updated to a version
      // which is still not the latest, and we are trying to update it again.
    }
  }
  // Return the info word for this Cloud.  Use the cache if possible
  public long cloud_info( H2O cloud ) {
    long x = _cache;
    // See if cached for this Cloud.  This should be the 99% fast case.
    if( cloud(x) == cloud._idx ) return x;

    // Cache missed!  Probaby it just needs (atomic) updating.
    // But we might be holding the stale cloud...
    // Figure out home Node in this Cloud
    char home = (char)cloud.D(this,0);
    // Figure out what replica # I am, if any
    int desired = desired(x);
    int replica = -1;
    for( int i=0; i<desired; i++ ) {
      int idx = cloud.D(this,i);
      if( idx != -1 && cloud._memary[idx] == H2O.SELF ) {
        replica = i;
        break;
      }
    }
    long cache = build_cache(cloud._idx,home,replica,desired);
    set_cache(cache);           // Attempt to upgrade cache, but ignore failure
    return cache;               // Return the magic word for this Cloud
  }

  // Default desired replication factor.  Unless specified otherwise, all new
  // k-v pairs start with this replication factor.
  public static final byte DEFAULT_DESIRED_REPLICA_FACTOR = 2;
  
  // Construct a new Key.
  private Key(byte[] kb) {
    if( kb.length > KEY_LENGTH ) throw new IllegalArgumentException();
    _kb = kb;
    // Quicky hash: http://en.wikipedia.org/wiki/Jenkins_hash_function
    int hash = 0;
    hash += (hash << 10);
    hash ^= (hash >> 6);
    // then the key bytes
    for( int i=0; i<kb.length; i++ ) {
      hash += kb[i];
      hash += (hash << 10);
      hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    _hash = hash;
  }

  // Make new Keys.  Optimistically attempt interning, but no guarantee.
  static public Key make(byte[] kb, byte rf) {
    Key key = new Key(kb);
    Key key2 = H2O.getk(key);   // Get the interned version, if any
    if( key2 != null )          // There is one!  Return it instead
      return key2;
    // Set the cache with desired replication factor, and a fake cloud index
    H2O cloud = H2O.CLOUD;      // Read once
    key._cache = build_cache(cloud._idx-1,0,0,rf);
    key.cloud_info(cloud);      // Now compute & cache the real data
    return key;
  }
  static public Key make(byte[] kb) { return make(kb,DEFAULT_DESIRED_REPLICA_FACTOR); }
  static private Key make(byte[] kb, int off, int len, byte rf) { return make(Arrays.copyOfRange(kb,off,off+len),rf); }
  static public Key make(String s) { return make(s.getBytes());}
  static public Key make(String s, byte rf) { return make(s.getBytes(), rf);}
  static public Key make() { return make( UUID.randomUUID().toString() ); }

  // Make a particular system key that is homed to given node and possibly
  // specifies also other 2 replicas. Works for both IPv4 and IPv6 addresses.
  // If the addresses are not specified, returns a key with no home information.
  static public Key make(String s, byte rf, byte systemType, H2ONode... replicas) {
    return make(s.getBytes(),rf,systemType,replicas);
  }
  

  static public Key make(byte[] kb, byte rf, byte systemType, H2ONode... replicas) {
    assert (replicas.length<=3); // no more than 3 replicas allowed to be stored in the key
    assert (systemType<32); // only system keys allowed
    throw new Error("unimplemented");
    //byte[] rr = makeReplicaRecord(replicas);
    //byte[] nkb = new byte[1+rr.length+kb.length];
    //assert (nkb.length<=KEY_LENGTH);
    //nkb[0] = systemType;
    //System.arraycopy(rr,0,nkb,1,rr.length);
    //System.arraycopy(kb,0,nkb,1+rr.length,kb.length);
    //return make(nkb,rf);
  }

  // User keys must be all ASCII, but we only check the 1st byte
  public boolean user_allowed() {
    return (_kb[0]&0xFF) >= 32;
  }
  
  // Returns the type of the key.
  public int type() {
    return ((_kb[0]&0xff)>=32) ? USER_KEY : (_kb[0]&0xff);
  }

  /** Converts the key to HTML displayable string.
   * 
   * For user keys returns the key itself, for system keys returns their
   * hexadecimal values.
   * 
   * @return key as a printable string
   */
  public String toString() {
    if( _kb[0]>=32)             // Normal keys
      return new String(_kb);
    // System keys
    StringBuilder sb = new StringBuilder(_kb.length*2+5);
    sb.append("(");
    sb.append(_kb.length);
    sb.append(")");
    for (byte b : _kb) {
      String s = Integer.toHexString((int)b & 0xff);
      if (s.length()==1)
        sb.append("0");
      sb.append(s);
    }
    return sb.toString();
  }


  public int hashCode() { return _hash; }
  public boolean equals( Object o ) {
    if( this == o ) return true;
    Key k = (Key)o;
    return Arrays.equals(k._kb,_kb);
  }

  // ---------------------------------------------------------------------
  // Known-achieved replication factor.  This is a cache of the first 8 Nodes
  // that have "checked in" with disk completion.  Monotonically increases over
  // time; limit of 8 replicas.  Tossed out on an update.  Implemented as 8 bytes
  // of dense integer indices, H2ONode._unique_idx;
  static private boolean cache_has_overflowed( long d ) {
    return (d>>>56)!=0;         // Overflowed if last byte is set
  }

  // Return the cache slot shift of this node, or a blank slot, or 64 if full
  static private int get_byte_shift( H2ONode h2o, long d ) {
    final char hidx = (char)h2o._unique_idx;
    assert hidx < 255;
    for( int i=0; i<64; i+=8 ) {       // 8 bytes of cache
      char idx = (char)((d>>>i)&0xFF); // Unsigned byte unique index being cached
      if( hidx == idx ) return i; // Found match
      if( idx == 0 ) return i;    // Found blank
    }
    return 64;                  // Missed
  }
  static boolean is_replica( long d, H2ONode h2o ) {
    int idx = get_byte_shift(h2o,d);
    if( idx == 64 ) return false;
    return ((d>>>idx)&0xff) != 0;
  }
  boolean is_disk_replica( H2ONode h2o ) { return is_replica(_disk_replicas,h2o); }
  boolean  is_mem_replica( H2ONode h2o ) { return is_replica( _mem_replicas,h2o); }

  static long set_replica( long d, H2ONode h2o ) {
    assert h2o != H2O.SELF;     // This is always for REMOTE replicas & caching
    int idx = get_byte_shift(h2o,d);
    if( idx < 64 )                         // Cache not full
      d |= (((long)h2o._unique_idx)<<idx); // cache it
    return d;
  }
  // Only the HOME node for a key tracks replicas
  void set_disk_replica( H2ONode h2o ) { assert home(); _disk_replicas=set_replica(_disk_replicas,h2o); }
  void  set_mem_replica( H2ONode h2o ) { assert home();  _mem_replicas=set_replica( _mem_replicas,h2o); }

  static long clr_replica( long d, H2ONode h2o ) {
    int idx = get_byte_shift(h2o,d);
    if( idx < 64 )              // Find in cache?
      d &= ~(0xFFL<<idx);       // Nuke it
    return d;
  }
  void clr_disk_replica( H2ONode h2o ) { _disk_replicas=clr_replica(_disk_replicas,h2o); }
  void  clr_mem_replica( H2ONode h2o ) {  _mem_replicas=clr_replica( _mem_replicas,h2o); }

  // Query the known level of persistence.  Only counts up to 8 known replicas right now.
  public int count_disk_replicas() {
    long d = _disk_replicas;
    H2O cloud = H2O.CLOUD;
    int repl = 0;
    while( d != 0 ) {
      int idx = (int)(d&0xFF);// Unsigned byte unique index being cached
      // Node being cached
      H2ONode h2o = H2ONode.IDX.get(idx);
      // Only report if is in the current Cloud
      if( cloud._memset.contains(h2o) ) repl++;
      d >>= 8;
    }
    return repl;
  }

  public String print_replicas() {
    String s= "{";
    long d = _disk_replicas;
    H2O cloud = H2O.CLOUD;
    while( d != 0 ) {
      int idx = (int)(d&0xFF);// Unsigned byte unique index being cached
      d >>= 8;
      H2ONode h2o = H2ONode.IDX.get(idx);
      s += h2o + ", ";
    }
    return s+"}";
  }

  // Returns true if the value is stored on the local disk. 
  boolean is_disk_local() { return is_disk_replica( H2O.SELF); }

  // Inform all the cached copies of this key, that it has changed.  This is a
  // non-blocking invalidate (but returns the task to block for it).
  void invalidate_remote_caches() {
    assert home();   // Only home node tracks mem replicas & issue invalidates
    long d = _mem_replicas;
    if( cache_has_overflowed(d) ) 
      throw new Error("unimplemented: bulk invalidate for key="+this);
    while( d != 0 ) {
      int idx = (int)(d&0xff);
      d >>= 8;
      H2ONode h2o = H2ONode.IDX.get(idx);
      invalidate(h2o);
    }
  }
  void invalidate(H2ONode target) {
    assert target != H2O.SELF;   // No point in tracking self, nor invalidating self
    throw new Error("unimplemented: invalidating "+target+" for key="+this);
  }


  // --------------------------------------------------------------------------
  // Read/Write keys in UDP packets
  // Wire-line length of Key.  2 bytes of length, then length bytes
  int wire_len() {
    return
      1+ // for replication factor
      2+ // for len
      _kb.length;
  }
  
  // Write the key length & bytes into a UDP packet
  int write( byte[] buf, int off ) {
    buf[off++] = (byte)desired();
    off += UDP.set2(buf,off,_kb.length);
    System.arraycopy(_kb,0,buf,off,_kb.length);
    off += _kb.length;
    //assert off < MultiCast.MTU; <- valid only for UDP packets, but this method
    // is used to write to any byte []
    return off;
  }
  
  // Read the key length & kind & bytes from a UDP packet.  Build a bare key.
  static Key read( byte[] buf, int off ) {
    byte rf = buf[off++];
    int len = UDP.get2(buf,off);  off += 2;
    return make( buf, off, len, rf);
  }
  
  // Write the Key to the Stream
  void write( DataOutputStream dos ) throws IOException {
    dos.writeByte(desired());
    dos.writeShort(_kb.length);
    dos.write(_kb,0,_kb.length);
  }

  // Read the Key, and some bytes of Value from Stream
  static Key read( DataInputStream dis ) throws IOException {
    byte rf = dis.readByte();
    int klen = dis.readShort();
    byte[] kb = new byte[klen];
    dis.readFully(kb);
    return make(kb, rf);
  }

  public int compareTo(Object o) {
    assert (o instanceof Key);
    return this.toString().compareTo(o.toString());
  }
  
}
