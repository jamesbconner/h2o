package water;
import java.io.*;
import java.net.*;
import java.util.*;

import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinWorkerThread;
import water.H2ONode.H2Okey;
import water.exec.Function;
import water.hdfs.Hdfs;
import water.nbhm.NonBlockingHashMap;

/**
 * Start point for creating or joining an <code>H2O</code> Cloud.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public final class H2O {

  static boolean _hdfsActive = false;

  static final String VERSION = "v0.3";

  // User name for this Cloud
  public static String NAME;

  // The default port for finding a Cloud
  static final int DEFAULT_PORT = 54321;
  public static int WEB_PORT;  // The HTML/web interface port - attach your browser here!
  public static int UDP_PORT;  // Fast/small UDP transfers
  public static int TCP_PORT;  // TCP port for long/large internal transfers

  // The multicast discovery port
  static NetworkInterface CLOUD_MULTICAST_IF;
  static InetAddress      CLOUD_MULTICAST_GROUP;
  static int              CLOUD_MULTICAST_PORT ;

  // Myself, as a Node in the Cloud
  public static H2ONode SELF = null;

  // Initial arguments
  public static String[] ARGS;

  public static final PrintStream OUT = System.out;
  public static final PrintStream ERR = System.err;

  // Convenience error
  public static final RuntimeException unimpl() { return new RuntimeException("unimplemented"); }

  // --------------------------------------------------------------------------
  // The Current Cloud.  A list of all the Nodes in the Cloud.  Changes if we
  // decide to change Clouds via atomic Cloud update.
  static public volatile H2O CLOUD;

  // ---
  // A Set version of the members.
  public final HashSet<H2ONode> _memset;
  // A dense array indexing all Cloud members.  Fast reversal from "member#" to
  // Node.  No holes.  Cloud size is _members.length.
  public final H2ONode[] _memary;
  // UUID to uniquely identify this cloud during paxos voting
  public final UUID _id;
  // A dense integer identifier that rolls over rarely.  Rollover limits the
  // number of simultaneous nested Clouds we are operating on in-parallel.
  // Really capped to 1 byte, under the assumption we won't have 256 nested
  // Clouds.  Capped at 1 byte so it can be part of an atomically-assigned
  // 'long' holding info specific to this Cloud.
  public final char _idx;       // no unsigned byte, so unsigned char instead

  // Is nnn larger than old (counting for wrap around)?  Gets confused if we
  // start seeing a mix of more than 128 unique clouds at the same time.  Used
  // to tell the order of Clouds appearing.
  static public boolean larger( int nnn, int old ) {
    assert (0 <= nnn && nnn <= 255);
    assert (0 <= old && old <= 255);
    return ((nnn-old)&0xFF) < 64;
  }

  // Static list of acceptable Cloud members
  static HashSet<H2ONode> STATIC_H2OS = null;

  // Reverse cloud index to a cloud; limit of 256 old clouds.
  static private final H2O[] CLOUDS = new H2O[256];

  // Construct a new H2O Cloud from the member list
  public H2O( UUID cloud_id, HashSet<H2ONode> memset, int idx ) {
    _id = cloud_id;             // Set the Cloud identity
    _memset = memset;           // Record membership list
    _memary = memset.toArray(new H2ONode[memset.size()]); // As an array
    Arrays.sort(_memary);       // ... sorted!
    _idx = (char)(idx&0x0ff);   // Roll-over at 256
  }

  // One-shot atomic setting of the next Cloud, with an empty K/V store.
  // Called single-threaded from Paxos.  Constructs the new H2O Cloud from a
  // member list.
  void set_next_Cloud( UUID id, HashSet<H2ONode> members) {
    synchronized(this) {
      int idx = _idx+1;         // Unique 1-byte Cloud index
      if( idx == 256 ) idx=1;   // wrap, avoiding zero
      H2O cloud = CLOUD = new H2O(id,members,idx);
      CLOUDS[idx] = cloud;   // Also remember here
    }
    Paxos.print("Announcing new Cloud Membership: ",members,"");
  }

  // Check if the cloud id matches with one of the old clouds
  static boolean isIDFromPrevCloud(H2ONode h2o) {
    if ( h2o == null ) return false;
    long lo = h2o.get_cloud_id_lo();
    long hi = h2o.get_cloud_id_hi();
    for( int i=0; i < 256; i++ )
      if( (CLOUDS[i] != null) &&
          (lo == CLOUDS[i]._id.getLeastSignificantBits() &&
           hi == CLOUDS[i]._id.getMostSignificantBits()))
        return true;
    return false;
  }

  public final int size() { return _memary.length; }

  // *Desired* distribution function on keys & replication factor.  Replica #0
  // is the master, replica #1, 2, 3, etc represent additional desired
  // replication nodes.  Note that this function is just the distribution
  // function - it does not DO any replication, nor does it dictate any policy
  // on how fast replication occurs.  Returns -1 if the desired replica
  // is nonsense, e.g. asking for replica #3 in a 2-Node system.
  public int D( Key key, int repl ) {
    if( repl >= size() ) return -1;

    // See if this is a specifically homed Key
    byte[] kb = key._kb;
    if( !key.user_allowed() && repl < kb[1] ) { // Asking for a replica# from the homed list?
      // Get the specified home
      H2ONode h2o = H2ONode.read(kb,(1+1+H2ONode.wire_len()*repl));
      // Reverse the home to the index
      int idx = nidx(h2o);
      if( idx != -1 ) return idx;
      // Else homed to a node which is no longer in the cloud!
      // Fall back to the normal home mode
    }

    // Easy Cheesy Stupid:
    return ((key._hash+repl)&0x7FFFFFFF) % size();
  }

  // Find the node index for this H2ONode.  Not so cheap.
  public int nidx( H2ONode h2o ) {
    for( int i=0; i<_memary.length; i++ )
      if( _memary[i]==h2o )
        return i;
    return -1;
  }

  // --------------------------------------------------------------------------
  // The (local) set of Key/Value mappings.
  static final NonBlockingHashMap<Key,Value> STORE = new NonBlockingHashMap<Key, Value>();

  // Dummy shared volatile for ordering games
  static public volatile int VOLATILE;

  // PutIfMatch
  // - Atomically update the STORE, returning the old Value on success
  // - Kick the persistence engine as needed
  // - Return existing Value on fail, no change.
  //
  // Keys are interned here: I always keep the existing Key, if any.  The
  // existing Key is blind jammed into the Value prior to atomically inserting
  // it into the STORE and interning.
  //
  // Because of the blind jam, there is a narrow unusual race where the Key
  // might exist but be stale (deleted, mapped to a TOMBSTONE), a fresh put()
  // can find it and jam it into the Value, then the Key can be deleted
  // completely (e.g. via an invalidate), the table can resize flushing the
  // stale Key, an unrelated weak-put can re-insert a matching Key (but as a
  // new Java object), and delete it, and then the original thread can do a
  // successful put_if_later over the missing Key and blow the invariant that a
  // stored Value always points to the physically equal Key that maps to it
  // from the STORE.  If this happens, some of replication management bits in
  // the Key will be set in the wrong Key copy... leading to extra rounds of
  // replication.

  public static final Value putIfMatch( Key key, Value val, Value old ) {
    assert val==null || val._key == key; // Keys matched
    if( old != null && val != null )     // Have an old value?
      key = val._key = old._key;         // Use prior key in val

    // Insert into the K/V store
    Value res = STORE.putIfMatchUnlocked(key,val,old);
    assert chk_equals_key(res, old);
    if( res != old )            // Failed?
      return res;               // Return the failure cause
    assert chk_equals_key(res, old, val);
    // Persistence-tickle.
    // If the K/V mapping is going away, remove the old guy.
    // If the K/V mapping is changing, let the store cleaner just overwrite.
    // If the K/V mapping is new, let the store cleaner just create
    if( old != null && val == null ) old.remove_persist(); // Remove the old guy
    if( val != null ) kick_store_cleaner(); // Start storing the new guy
    return old;                 // Return success
  }

  // assert that all of val, old & res that are not-null all agree on key.
  private static final boolean chk_equals_key( Value... vs ) {
    Key k = null;
    for( Value v : vs ) {
      if( v != null ) {
        assert k == null || k == v._key;
        k = v._key;
      }
    }
    return true;
  }

  // Raw put; no marking the memory as out-of-sync with disk.  Used to import
  // initial keys from local storage, or to intern keys.
  public static final Value putIfAbsent_raw( Key key, Value val ) {
    assert val.is_same_key(key);
    Value res = STORE.putIfMatchUnlocked(key,val,null);
    assert res == null;
    return res;
  }

  // Get the value from the store
  public static Value get( Key key ) {
    Value v = STORE.get(key);
    // Lazily manifest array chunks, if the backing file exists.
    if( v == null ) {
      v = Value.lazy_array_chunk(key);
      if( v == null ) return null;
      // Insert the manifested value, as-if it existed all along
      Value res = putIfMatch(key,v,null);
      if( res != null ) v = res; // This happens racily, so take any prior result
    }
    if( v != null ) v.touch();
    return v;
  }

  public static Value raw_get( Key key ) { return STORE.get(key); }
  public static Key getk( Key key ) { return STORE.getk(key); }
  public static Set<Key> keySet( ) { return STORE.keySet(); }
  public static Collection<Value> values( ) { return STORE.values(); }
  public static int store_size() { return STORE.size(); }


  // --------------------------------------------------------------------------
  // The main Fork/Join worker pool(s).
  static class FJWThr extends ForkJoinWorkerThread {
    FJWThr(ForkJoinPool pool, int priority) {
      super(pool);
      setPriority(priority);
    }
  }
  static class FJWThrFact implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    final int _priority;
    FJWThrFact( int priority ) { _priority = priority; }
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      // The "Normal" or "Low" priority work queues get capped at 99 threads
      // blocked on I/O.  I/O work *should* be done by HI priority threads on
      // other nodes - so hopefully this will not lead to deadlock.  Capping
      // all thread pools definitely does lead to deadlock.
      return (_priority > Thread.MIN_PRIORITY || pool.getPoolSize() < 100)
        ? new FJWThr(pool,_priority) : null;
    }
  }
  // Hi-priority work is things that block other things, eg. TaskGetKey, and
  // typically does I/O.
  public static final ForkJoinPool FJP_HI =
    new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                     new FJWThrFact(Thread.MAX_PRIORITY-2), null, false);

  // Normal-priority work is generally directly-requested user ops.
  public static final ForkJoinPool FJP_NORM =
    new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
                     new FJWThrFact(Thread.MIN_PRIORITY), null, false);

  // --------------------------------------------------------------------------
  public static OptArgs OPT_ARGS = new OptArgs();
  public static class OptArgs extends Arguments.Opt {
    public String name;               // set_cloud_name_and_mcast()
    public String flatfile;           // set_cloud_name_and_mcast()
    public int port;                  // set_cloud_name_and_mcast()
    public String ip;                 // Named IP4/IP6 address instead of the default
    public String ice_root;           // ice root directory
    public String hdfs;               // HDFS backend
    public String hdfs_version;       // version of the filesystem
    public String hdfs_root;          // root of the HDFS installation (for I only)
    public String hdfs_config;        // configuration file of the HDFS
    public String hdfs_datanode;      // Datanode root
    public String hdfs_nopreload;     // do not preload HDFS keys
    public String nosigar;            // Disable Sigar-based statistics
    public String keepice;            // Do not delete ice on startup
    public String auth;               // Require authentication for the webpages
  }
  public static boolean IS_SYSTEM_RUNNING = false;

  // Start up an H2O Node and join any local Cloud
  public static void main( String[] args ) {
    // To support launching from JUnit, JUnit expects to call main() repeatedly.
    // We need exactly 1 call to main to startup all the local services.
    if (IS_SYSTEM_RUNNING) return;
    IS_SYSTEM_RUNNING = true;

    // Parse args
    Arguments arguments = new Arguments(args);
    arguments.extract(OPT_ARGS);
    ARGS = arguments.toStringArray();

    // Redirect System.out/.err to the Log system and collect them in LogHub
    LogHub.prepare_log_hub();
    Log.hook_sys_out_err();

    startLocalNode();     // start the local node
    // Load up from disk and initialize the persistence layer
    initializePersistence();
    // Start network services, including heartbeats & Paxos
    startNetworkServices();  // start server services

    initializeExpressionEvaluation(); // starts the expression evaluation system

    startupFinalize();    // finalizes the startup & tests (if any)
    // Hang out here until the End of Time
    // test if we have multicast
    try {Thread.sleep(20000);} catch( InterruptedException e ) {}
    if (OPT_ARGS.flatfile==null && CLOUD._memary.length == 1) {
      if(MultiReceiverThread.receivedMCastMsgCount == 0){
        System.err.println("WARNING: No other nodes are visible. No flatfile argument and no multicast messages received. Broken multicast?");
      }
    } else {
      for(H2ONode n:H2O.CLOUD._memary){
        if(n == H2O.SELF)continue;
        if(n._last_heard_from == 0)System.err.println("Never heard from " + n);
      }
    }
  }

  private static void initializeExpressionEvaluation() {
    Function.initializeCommonFunctions();
  }

  /** Starts the local k-v store.
   * Initializes the local k-v store, local node and the local cloud with itself
   * as the only member.
   *
   * @param args Command line arguments
   * @return Unprocessed command line arguments for further processing.
   */
  private static void startLocalNode() {
    // Figure self out; this is surprisingly hard
    set_cloud_name_and_mcast();
    SELF = H2ONode.self();
    // Do not forget to put SELF into the static configuration (to simulate
    // proper multicast behavior)
    if( STATIC_H2OS != null )
      STATIC_H2OS.add(SELF);

    System.out.println("[h2o] ("+VERSION+") '"+NAME+"' on " + SELF+
                       (OPT_ARGS.flatfile==null
                        ? (", discovery address "+CLOUD_MULTICAST_GROUP+":"+CLOUD_MULTICAST_PORT)
                        : ", static configuration based on -flatfile "+OPT_ARGS.flatfile));

    // Create the starter Cloud with 1 member
    HashSet<H2ONode> starter = new HashSet<H2ONode>();
    starter.add(SELF);
    CLOUD = new H2O(UUID.randomUUID(),starter,1);
  }

  /** Initializes the network services of the local node.
   *
   * Starts the worker threads, receiver threads, heartbeats and all other
   * network related services.
   */
  private static void startNetworkServices() {
    // We've rebooted the JVM recently.  Tell other Nodes they can ignore task
    // prior tasks by us.  Do this before we receive any packets
    UDPRebooted.build_and_multicast();

    // Start the UDPReceiverThread, to listen for requests from other Cloud
    // Nodes.  There should be only 1 of these, and it never shuts down.
    // Started first, so we can start parsing UDP packets
    new UDPReceiverThread().start();

    // Start the MultiReceiverThread, to listen for multi-cast requests from
    // other Cloud Nodes.  There should be only 1 of these, and it never shuts
    // down.  Started soon, so we can start parsing multicast UDP packets
    new MultiReceiverThread().start();

    // Start the heartbeat thread, to publish the Clouds' existence to other
    // Clouds.  This will typically trigger a round of Paxos voting so we can
    // join an existing Cloud.
    final Thread hbt = new HeartBeatThread();
    hbt.setDaemon(true);
    hbt.start();

    // Start a UDP timeout worker thread.  This guy only handles requests for
    // which we have not recieved a timely response and probably need to
    // arrange for a re-send to cover a dropped UDP packet.
    new UDPTimeOutThread().start();

    // Start the TCPReceiverThread, to listen for TCP requests from other Cloud
    // Nodes.  There should be only 1 of these, and it never shuts down.
    new TCPReceiverThread().start();

    // Start the Persistent meta-data cleaner thread, which updates the K/V
    // mappings periodically to disk.  There should be only 1 of these, and it
    // never shuts down.
    new Cleaner().start();

    water.web.Server.start();
  }

  /** Finalizes the node startup.
   *
   * Displays the startup message and runs the tests (if applicable).
   */
  private static void startupFinalize() {
    // Sleep a bit so all my other threads can 'catch up'
    try { Thread.sleep(1000); } catch( InterruptedException e ) { }
  }

  // Parse arguments and set cloud name in any case.  Strip out "-name NAME"
  // and "-flatfile <filename>".  Ignore the rest.  Set multi-cast port as a hash
  // function of the name.  Parse node ip addresses from the filename.
  static void set_cloud_name_and_mcast( ) {
    // Assign initial ports
    WEB_PORT = OPT_ARGS.port != 0 ? OPT_ARGS.port : DEFAULT_PORT;
    UDP_PORT = WEB_PORT+1;
    TCP_PORT = WEB_PORT+2;

    // See if we can grab them without bind errors!
    while( !(test_port(WEB_PORT) && test_port(UDP_PORT) && test_port(TCP_PORT)) ) {
      if( OPT_ARGS.port != 0 )
        Log.die("On " + H2ONode.findInetAddressForSelf() +
            " some of the required ports " + (OPT_ARGS.port+0) +
            ", "                           + (OPT_ARGS.port+1) +
            ", and "                       + (OPT_ARGS.port+2) +
            " are not available, change -port PORT and try again.");

      // Try the next available port(s)
      WEB_PORT++; UDP_PORT++; TCP_PORT++;
    }

    System.out.println("[h2o] HTTP listening on port: "+WEB_PORT+", UDP port: "+UDP_PORT+", TCP port: "+TCP_PORT);

    NAME = OPT_ARGS.name==null?  System.getProperty("user.name") : OPT_ARGS.name;
    // Read a flatfile of allowed nodes
    STATIC_H2OS = from_file(OPT_ARGS.flatfile);

    // Multi-cast ports are in the range E1.00.00.00 to EF.FF.FF.FF
    int hash = NAME.hashCode()&0x7fffffff;
    int port = (hash % (0xF0000000-0xE1000000))+0xE1000000;
    byte[] ip = new byte[4];
    for( int i=0; i<4; i++ )
      ip[i] = (byte)(port>>>((3-i)<<3));
    try {
      CLOUD_MULTICAST_GROUP = InetAddress.getByAddress(ip);
    } catch( UnknownHostException e ) { throw new Error(e); }
    CLOUD_MULTICAST_PORT = (port>>>16);
  }

  // Read a set of Nodes from a file.  Example:
  //   # this is a comment
  //   10.10.65.105:54322
  private static HashSet<H2ONode> from_file( String fname ) {
    if( fname == null ) return null;
    File f = new File(fname);
    if( !f.exists() ) return null; // No flat file
    HashSet<H2ONode> h2os = new HashSet<H2ONode>();
    BufferedReader br = null;
    int port=-1;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
      String strLine = null;
      while( (strLine = br.readLine()) != null) {
        // be user friendly and skip comments
        if (strLine.startsWith("#")) continue;
        final String[] ss = strLine.split("[/:]");
        if( ss.length!=3 )
          Log.die("Invalid format, must be name/ip:port, not '"+strLine+"'");

        final InetAddress inet = InetAddress.getByName(ss[1]);
        if( !(inet instanceof Inet4Address) )
          Log.die("Only IP4 addresses allowed.");
        try {
          port = Integer.decode(ss[2]);
        } catch( NumberFormatException nfe ) {  Log.die("Invalid port #: "+ss[2]); }
        h2os.add(H2ONode.intern(inet,port));
        //h2os.add(new H2ONode(inet,port));
      }
    } catch( Exception e ) {  Log.die(e.toString()); }
    finally { try { br.close(); } catch( IOException e ) { /* nasty ignore */ } };
    return h2os;
  }

  // Test if this port is available
  static boolean test_port( int port ) {
    boolean res = true;
    ServerSocket ss = null;
    try {
      ss = new ServerSocket(port);
    } catch( IOException se ) {
      res = false;
    } finally {
      if( ss != null )
        try { ss.close(); } catch( IOException se ) { }
    }
    return res;
  }

  static void initializePersistence() {
    PersistIce.initialize();
    PersistNFS.initialize();
    if( OPT_ARGS.hdfs!=null ) Hdfs.initialize();
  }


  // Cleaner ---------------------------------------------------------------

  static boolean _dirty;

  static void kick_store_cleaner() {
    synchronized(STORE) {
      _dirty = true;
      STORE.notifyAll();
    }
  }

  // Periodically write user keys to disk
  public static class Cleaner extends Thread {
    public Cleaner() { super("Memory Cleaner"); }

    long [] histogram = new long [128];
    int maxTime;
    int currentMaxTime;
    int hStep;

    public void run() {
      int cycles_no_progress = 0;
      while (true) {
        synchronized (STORE) {
          while( _dirty == false && (MemoryManager.mem2Free >> 20) <= 0 )
            try { STORE.wait(); } catch (InterruptedException ie) { }
          _dirty = false;       // Clear the flag, about to clean
        } // Release lock
        // If not out of memory, sleep another second to batch-up writes
        if( (MemoryManager.mem2Free >> 20) <= 0 )
          try { Thread.sleep(5000); } catch (InterruptedException e) { }
        // obtain the expected age of "old" values to be removed from memory
        long s = 0;
        int idx = histogram.length-1;

        int old = maxTime;
        while (idx >= 0 && ((s + histogram[idx]) <= MemoryManager.mem2Free)) {
          s += histogram[idx];
          old -= hStep;
          --idx;
        }
        Arrays.fill(histogram, 0);
        maxTime = currentMaxTime;
        if(maxTime == 0){
          maxTime = 1024*histogram.length;
          hStep = 1024;
          old = Integer.MAX_VALUE;
        } else hStep = Math.max(1,maxTime/histogram.length);
        currentMaxTime = 0;
        System.out.println("old = " + old + ", maxTime = " + maxTime + " hstep = " + hStep);

        boolean cleaned = false; // Anything got cleaned?
        long cacheSz = 0;       // Current size of cached memory
        final long currentTime = System.currentTimeMillis();
        // For faster K/V store walking get the NBHM raw backing array,
        // and walk it directly.
        Object[] kvs = STORE.raw_array();
        // Start the walk at slot 2, because slots 0,1 hold meta-data
        for( int i=2; i<kvs.length; i += 2 ) {
          // In the raw backing array, Keys and Values alternate in slots
          Object ok = kvs[i+0], ov = kvs[i+1];
          if( !(ok instanceof Key  ) ) continue; // Ignore tombstones and Primes and null's
          Key   key = (Key  )ok;
          if( !(ov instanceof Value) ) continue; // Ignore tombstones and Primes and null's
          Value val = (Value)ov;
          byte[] m = val._mem;
          if( m == null ) continue;
          cacheSz += m.length;  // Accumulate total amount of cached keys
          if( m.length < val._max )
            continue;           // Do not persist partial keys

          // System keys that are not just backing arraylets of user keys are
          // not persisted - we figure they have a very short lifetime.
          if( !MemoryManager.memCritical() ) { // if memory is critical, persist and free system keys also.
            if( key._kb[0] != Key.ARRAYLET_CHUNK )  // Not arraylet?
              continue; // Not enough savings to write it with mem-pressure to force us
            // If this is a chunk of a system-defined array, then assume it has
            // short lifetime, and we do not want to spin the disk writing it
            // unless we're under memory pressure.
            Key arykey = Key.make(ValueArray.getArrayKeyBytes(key));
            if( !arykey.user_allowed() )
              continue; // System array chunk?
          } else {      // Under memory pressure?
            // Now write to disk everything, except sys non-array chunks - this
            // are typically both very small and very short lived.
            if( key._kb[0] != Key.ARRAYLET_CHUNK && !key.user_allowed() )
              continue;
          }
          // ValueArrays covering large files in global filesystems such as NFS
          // or HDFS are only made on import (right now), and not reconstructed
          // by inspection of the Key or filesystem.... so we cannot toss them
          // out because they will not be reconstructed merely by loading the
          // Value.
          if( val instanceof ValueArray &&
              (val._persist & Value.BACKEND_MASK)!=Value.ICE )
            continue;

          // Store user-keys, or arraylets from user-keys
          val.store_persist();
          int age = (int)(System.currentTimeMillis() - val._lastAccessedTime);
          if(age >= old &&  MemoryManager.removeValue(val) ) {
            cacheSz -= m.length;
            cleaned = true;
          } else { // update the histogram
            if(age > currentMaxTime)currentMaxTime = age;
            histogram[Math.min(age/hStep,histogram.length-1)] += m.length;
          }
        }
        if( cleaned ) cycles_no_progress = 0;
        else cycles_no_progress++;
        // update the cache sz
        MemoryManager.setCacheSz(cacheSz, cycles_no_progress > 5);
        if( cycles_no_progress > 5 ) {
          cycles_no_progress = 0;
          try { Thread.sleep(1000); } catch (InterruptedException e) { }
        }
      }
    }
  }
}
