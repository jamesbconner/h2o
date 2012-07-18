package water;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import jsr166y.ForkJoinPool;
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

  static final String version = "v0.3";
  
  // User name for this Cloud
  public static String NAME;
  
  // The default port for finding a Cloud
  static final int DEFAULT_PORT = 54321;
  public static int WEB_PORT;  // The HTML/web interface port - attach your browser here!
  public static int UDP_PORT;  // Fast/small UDP transfers
  public static int TCP_PORT;  // TCP port for long/large internal transfers

  // The multicast discovery port
  static InetAddress CLOUD_MULTICAST_GROUP;
  static int         CLOUD_MULTICAST_PORT ;

  // Myself, as a Node in the Cloud
  public static H2ONode SELF = null;

  // Initial arguments
  public static String[] ARGS;

  public static final PrintStream OUT = System.out;
  public static final PrintStream ERR = System.err;

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
  // Clouds.
  public final char _idx;       // no unsigned byte, so unsigned char instead

  // Is nnn larger than old, counting for wrap around?  Gets confused if we
  // start seeing a mix of more than 128 unique clouds at the same time.  Used
  // to tell the order of Clouds appearing.
  static public boolean larger( int nnn, int old ) {
    assert (0 <= nnn && nnn <= 255);
    assert (0 <= old && old <= 255);
    return ((nnn-old)&0xFF) < 64;
  }
 
  // A Set version of the static configuration InetAddress.
  static ArrayList<H2ONode> NODES = new ArrayList<H2ONode>();
  // Multicast is on by default
  static boolean MULTICAST_ENABLED = true;
  static String NODES_FILE = null;

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
    mark_dirty();               // Trigger a round of replication-cleaning
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
  static final NonBlockingHashMap<Key,Value> STORE = new NonBlockingHashMap();

  // Dummy shared volatile for ordering games
  static public volatile int VOLATILE;

  // Put-if-later.
  // - Will invoke tie-breaking logic if the vector-clocks are not comparable.
  // - Returns NULL if the existing Value has a newer VectorClock vs the
  // attempted put Value, and does not change the STORE.
  // - Returns the old value on success, also bumping the local Clock and
  // kicking the persistence engine.  Returns the old value if the old value is
  // semantically the same Value... without changing the vector clock.
  // - Weak puts from the SAME Node but different threads can have a narrow
  // race where they submit unrelated Values to the same Key with equal
  // VectorClocks; normally writes to the same Key will never have equal VCs.
  // In this case I act "as if" the new write "succeeded but was overwritten"
  // by the existing Value.
  //
  // If the old value is loaded and the new value is not, then the old value is
  // always kept. 
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

  public static final Value put_if_later( Key key, Value val ) {
    while( true ) {
      Value old = STORE.get(key);
      if( old != null ) {            // Have an old value at all?
        // if the old value is loaded and the new value is just a sentinel, keep
        // the old value always. 
        if( old == val ) return old; // Trivial success, perhaps dup UDP packets
        // Less trivial equality check; remove dup write attempts for the same Value
        if( old.same_clock(val) ) {
          // We can only get the SAME clock from racing unrelated writes on the
          // same Node, or if we're handed the same clock twice from e.g. dup UDP
          // packets writes.
          return ((old.mem() == val.mem()) || Arrays.equals(old.mem(),val.mem()))
            ? old          // Trivial success: dup write attempt of equal Value
            : val;         // Fail: racing unrelated weak-writes from same Node
        }
        if( val.happens_before(old) ) return null; // Existing is strictly newer
        if( !old.happens_before(val) ) {
          System.out.println("old vc="+old.vector_clock_string());
          System.out.println("new vc="+val.vector_clock_string());
          throw new Error("unimplemented: tie-breaker node needs to break this tie");
        }
      }
      // Interning: use the same One True Key found in the STORE.
      Key key2 = STORE.getk(key);
      if( key2 != null ) key = key2; // Use the existing Key, if any
      if (!val.is_sentinel())
        val._key = key;         // Ignore users key, use the interned key
      // Insert into the K/V store
      Value res = STORE.putIfMatchUnlocked(key,val,old);
      if( res == old ) {               // Success?
        if (!val.is_sentinel()) {
          assert (val._key == key);        // These match on success
          val.apply_max_vector_clock();  // Changed the STORE, so take max VC
          SELF._clock.getAndIncrement(); // Changed the STORE, so bump clock
        }
        key.invalidate_mem_caches();
        key.set_mem_replica(SELF); // RAM copy is here
        // initiate local persisting
        key.clr_disk_replicas(); // Not cached any disk
        key.is_local_persist(val,res); // Start persisting
        mark_dirty();            // Start the K/V repair sweeper
   
        return res;              // Return success
      }
      // Racing updates, just try again
    }
  }
  // Raw put; no bumping of vector clock or marking the memory as out-of-sync
  // with disk.  Used to import initial keys from local storage, or to intern keys.
  public static final Value putIfAbsent_raw( Key key, Value val ) {
    assert val.is_same_key(key);
    Value res = STORE.putIfMatchUnlocked(key,val,null);
    assert res == null;
    
    return res;
  }

  // Get the value from the store, if the value is a placeholder for a locally
  // stored value, replace it with a proper value and continue.  This WILL
  // return "deleted" Values.
  public static Value get( Key key ) {
    Value v = STORE.get(key);
    if(v != null)
      v.touch();
    if( (v==null || v.is_deleted()) || !v.is_sentinel())
      return v;
    Value vnew = v.load(key); // At least read size
    assert vnew.is_same_key(key);
    // Attempt to set it atomically in case of a racing put from another thread.
    return (STORE.putIfMatchUnlocked(key,vnew,v) == v)
      ? vnew                    // Success
      // Retry after failed put; it should never again be the sentinel ON_LOCAL_DISK
      : STORE.get(key);
  }
  public static Value raw_get( Key key ) { return STORE.get(key); }
  public static Key getk( Key key ) { return STORE.getk(key); }
  public static Set<Key> keySet( ) { return STORE.keySet(); }
  public static Collection<Value> values( ) { return STORE.values(); }


  // --------------------------------------------------------------------------
  // The main Fork/Join worker pool
  public static final ForkJoinPool FJP
    = new ForkJoinPool(4/*Runtime.getRuntime().availableProcessors()*/,
                       ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                       null, // handler for catching uncaught exceptions
                       false // stack-mode by default
                       );

  // --------------------------------------------------------------------------
  public static OptArgs OPT_ARGS;
  public static class OptArgs extends Arguments.Opt {
    public String name;               // set_cloud_name_and_mcast()
    public String nodes;              // set_cloud_name_and_mcast()
    public int port;                  // set_cloud_name_and_mcast()
    public String ip;
    public String ice_root;           // ice root directory
    public String hdfs;               // HDFS backend
    public String hdfs_root;          // root of the HDFS installation (for I only)
    public String hdfs_config;        // configuration file of the HDFS
    public String hdfs_datanode;      // Datanode root
  }

  // Start up an H2O Node and join any local Cloud
  public static void main( String[] args ) {
    // Parse args
    Arguments arguments = new Arguments(args);
    OptArgs oa = new OptArgs();
    arguments.extract(oa);
    ARGS = arguments.toStringArray();
    OPT_ARGS = oa;

    // Redirect System.out/.err to the Log system
    Log.hook_sys_out_err();

    startLocalNode();     // start the local node
    // Load up from disk and initialize the persistence layer
    initializePersistence();
    startNetworkServices();  // start server services
    startupFinalize();    // finalizes the startup & tests (if any)
    // Hang out here until the End of Time
  }


  /** Starts the local k-v store.
   * Initializes the local k-v store, local node and the local cloud with itself
   * as the only member. 
   * 
   * @param args Command line arguments
   * @return Unprocessed command line arguments for further processing.
   */
  private static void startLocalNode() {
    set_cloud_name_and_mcast();
    SELF = H2ONode.self();

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
   
    System.out.println("The Cloud '"+NAME+"' is Up ("+version+") on " + SELF+
                       (MULTICAST_ENABLED
                        ? (", discovery address "+CLOUD_MULTICAST_GROUP+":"+CLOUD_MULTICAST_PORT)
                        : ", configuration based on -nodes "+NODES_FILE));
  }
  

  // Parse arguments and set cloud name in any case.  Strip out "-name NAME"
  // and "-nodes <filename>".  Ignore the rest.  Set multi-cast port as a hash
  // function of the name.  Parse node ip addresses from the filename.
  static void set_cloud_name_and_mcast( ) {
    // Assign initial ports
    WEB_PORT = OPT_ARGS.port != 0 ? OPT_ARGS.port : DEFAULT_PORT;
    UDP_PORT = WEB_PORT+1;
    TCP_PORT = WEB_PORT+2;

    // See if we can grab them without bind errors!
    while( !(test_port(WEB_PORT) && test_port(UDP_PORT) && test_port(TCP_PORT)) ) {
      if( OPT_ARGS.port != 0 ) 
        Log.die("Some of the required ports "+(OPT_ARGS.port+0)+
                ", "+(OPT_ARGS.port+1)+
                ", and "+(OPT_ARGS.port+2)+
                "are not available, change -port PORT and try again.");
      WEB_PORT++;               // Try the next available port(s)
      UDP_PORT++;
      TCP_PORT++;
    }
    System.out.println("HTTP listening on port: "+WEB_PORT+", UDP port: "+UDP_PORT+", TCP port: "+TCP_PORT);

    NAME = OPT_ARGS.name==null?  System.getProperty("user.name") : OPT_ARGS.name;
    if (OPT_ARGS.nodes != null) {
      NODES_FILE = OPT_ARGS.nodes;
      MULTICAST_ENABLED = false;
    }
      
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

    if( !MULTICAST_ENABLED ) {
      try {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(NODES_FILE)));
        String strLine = null;
        while ((strLine = br.readLine()) != null)   {
          final String[] ss = strLine.split("[\\s:]");
          final InetAddress inet = InetAddress.getByName(ss[0]);
          if( !(inet instanceof Inet4Address) )
            Log.die("Only IP4 addresses allowed.");
          try {
            final int port2 = Integer.decode(ss[1]);
            NODES.add(H2ONode.intern(inet,port2));
          } catch( NumberFormatException nfe ) {
            Log.die("Invalid port #: "+ss[1]);
          }
        }
      } catch( Exception e ) {  Log.die(e.toString()); }
    }
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
    if (OPT_ARGS.hdfs!=null) Hdfs.initialize();
    // Note that we get a LocalPersist EVEN IF HDFS is out there... for our
    // internal non-HDFS keys.
    PersistIce.initialize();
  }

  // Cleaner API ---------------------------------------------------------------

  static AtomicBoolean _dirty = new AtomicBoolean();

  static void mark_dirty() {
    if( !_dirty.get() ) // Read-before-write to help with cache issues
      _dirty.set(true);
  }

  static final int MAX_REPLICATING_KEYS = 100;
  // Make sure the remote replicas are up-to-date periodically.
  public static class Cleaner extends Thread {

    public void run() {
      byte[] hazkeys_buffer = new byte[MultiCast.MTU];
      // Once/sec, write out the top-level K/V... if dirty.
      while( true ) {                
        // Persist all values... which also persists all metadata needed to reload
        H2O cloud = CLOUD;
        long now = System.currentTimeMillis();
        for( Key key : keySet() ) {
          byte [] k = key._kb;
          int keySize = (k != null)? k.length:0;
          key.cloud_info(cloud); // Update all cloud caches
          // fetch value withOUT loading it from disk
          Value val = raw_get(key);
          // Trigger any replication/repair action required
          if( !key.repair(cloud, hazkeys_buffer, val) )
            mark_dirty();       // Not yet all persisted
        }
        try { Thread.sleep(1000); } catch( InterruptedException e ) { }
      }
    }
  }
}
