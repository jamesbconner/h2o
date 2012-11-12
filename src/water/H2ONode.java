package water;

import init.Boot;

import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import jsr166y.ForkJoinPool;
import water.nbhm.NonBlockingHashMap;
import water.nbhm.NonBlockingHashMapLong;

/**
 * A <code>Node</code> in an <code>H2O</code> Cloud.
 * Basically a worker-bee with CPUs, Memory and Disk.
 * One of this is the self-Node, but the rest are remote Nodes.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class H2ONode implements Comparable {

  // A JVM is uniquely named by machine IP address and port#
  public static final class H2Okey {
    public final InetAddress _inet; // IP address for the remote JVM
    public final int _port;       // UDP port for the remote JVM
    public H2Okey(InetAddress inet, int port) { _inet = inet; _port = port; }
    public int tcp_port() { return _port+1; }
    // HashCode & equals
    public int hashCode() { return _inet.hashCode() ^ _port; }
    public boolean equals( Object o ) {
      if( !(o instanceof H2Okey) ) return false;
      H2Okey key = (H2Okey)o;
      return _port==key._port && _inet.equals(key._inet);
    }
    public String toString() { return _inet.toString()+":"+(_port-1); }
    static int wire_len() { return 4/*IP4 only*/+2/*port#*/; }
    int write( byte[] buf, int off ) {
      byte[] ip = _inet.getAddress();
      if( ip.length != 4 ) throw H2O.unimpl(); // ipv6 wire line protocol
      System.arraycopy(ip,0,buf,off,4);
      off += 4;
      buf[off++] = (byte)(_port>>0);
      buf[off++] = (byte)(_port>>8);
      return off;
    }
    static H2Okey read( byte[] buf, int off ) {
      byte[] ip4 = new byte[4];
      System.arraycopy(buf,off,ip4,0,4);
      InetAddress inet;
      try { inet = InetAddress.getByAddress(ip4); }
      catch( UnknownHostException e ) { throw new Error(e); }
      off += 4;
      int port = (buf[off+0]&0xff) + ((buf[off+1]&0xff)<<8);
      return new H2Okey(inet,port);
    }
  }
  public final H2Okey _key;

  public final int _unique_idx; // Dense integer index, skipping 0.
  public long _last_heard_from; // Time in msec since we last heard from this Node

  // The wire-line protocol health buffer
  byte[] _health_buf = new byte[hb.max.offset()];

  // These are INTERN'd upon construction, and are uniquely numbered within the
  // same run of a JVM.  If a remote Node goes down, then back up... it will
  // come back with the SAME IP address, and the same unique_idx and history
  // relative to *this* Node.  They can be compared with pointer-equality.  The
  // unique idx is used to know which remote Nodes have cached which Keys, even
  // if the Home#/Replica# change for a Key due to an unrelated change in Cloud
  // membership.  The unique_idx is *per Node*; not all Nodes agree on the same
  // indexes.
  private H2ONode( H2Okey key, int unique_idx ) {
    _key = key;
    _unique_idx = unique_idx;
    _last_heard_from = System.currentTimeMillis();
    // Nail down the buffer type
    UDP.set_ctrl(_health_buf,UDP.udp.heartbeat.ordinal());
    UDP.set_port(_health_buf,_key._port);
  }

  // ---------------
  // A dense integer index for every unique IP ever seen, since the JVM booted.
  // Used to track "known replicas" per-key across Cloud change-ups.  Just use
  // an array-of-H2ONodes, and a limit of 255 unique H2ONodes
  static private final NonBlockingHashMap<H2Okey,H2ONode> INTERN = new NonBlockingHashMap<H2Okey,H2ONode>();
  static private final AtomicInteger UNIQUE = new AtomicInteger(1);
  static public H2ONode IDX[] = new H2ONode[1];


  // Create and/or re-use an H2ONode.  Each gets a unique dense index, and is
  // *interned*: there is only one per InetAddress.
  public static final H2ONode intern( H2Okey key ) {
    H2ONode h2o = INTERN.get(key);
    if( h2o != null ) return h2o;
    final int idx = UNIQUE.getAndIncrement();
    h2o = new H2ONode(key,idx);
    H2ONode old = INTERN.putIfAbsent(key,h2o);
    if( old != null ) return old;
    synchronized(H2O.class) {
      if( idx >= IDX.length )
        IDX = Arrays.copyOf(IDX,IDX.length<<1);
      IDX[idx] = h2o;
    }
    return h2o;
  }
  public static final H2ONode intern( InetAddress ip, int port ) { return intern(new H2Okey(ip,port)); }

  public final int ip4() {
    byte[] b = _key._inet.getAddress();
    return
      (b[0] <<24) |
      (b[1] <<16) |
      (b[2] << 8) |
      (b[3] << 0);
  }

  static public int wire_len() { return H2Okey.wire_len(); }
  // Read & return interned from wire
  static H2ONode read( byte[] buf, int off ) { return intern(H2Okey.read(buf,off));  }
  int write( byte[] buf, int off ) { return _key.write(buf,off); }

  // Get a nice Node Name for this Node in the Cloud.  Basically it's the
  // InetAddress we use to communicate to this Node.
  static H2ONode self() {
    assert H2O.UDP_PORT != 0;
    InetAddress local = findInetAddressForSelf();

    try {
      // Figure out which interface matches our IP address
      List<NetworkInterface> matchingIfs = new ArrayList();
      Enumeration<NetworkInterface> netIfs = NetworkInterface.getNetworkInterfaces();
      while( netIfs.hasMoreElements() ) {
        NetworkInterface netIf = netIfs.nextElement();
        Enumeration<InetAddress> addrs = netIf.getInetAddresses();
        while( addrs.hasMoreElements() ) {
          InetAddress addr = addrs.nextElement();
          if( addr.equals(local) ) {
            matchingIfs.add(netIf);
            break;
          }
        }
      }
      switch( matchingIfs.size() ) {
      case 0: H2O.CLOUD_MULTICAST_IF = null; break;
      case 1: H2O.CLOUD_MULTICAST_IF = matchingIfs.get(0); break;
      default:
        System.err.print("Found multiple network interfaces for ip address " + local);
        for( NetworkInterface ni : matchingIfs ) {
          System.err.println("\t" + ni);
        }
        System.err.println("Using " + matchingIfs.get(0) + " for UDP broadcast");
        H2O.CLOUD_MULTICAST_IF = matchingIfs.get(0);
      }
    } catch( SocketException e ) {
      throw new RuntimeException(e);
    }
    return intern(new H2Okey(local,H2O.UDP_PORT));
  }
  static InetAddress findInetAddressForSelf() throws Error {
    // Get a list of all valid IPs on this machine.  Typically 1 on Mac or
    // Windows, but could be many on Linux or if a hypervisor is present.
    ArrayList<InetAddress> ips = new ArrayList<InetAddress>();
    try {
      Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
      while( nis.hasMoreElements() ) {
        NetworkInterface ni = nis.nextElement();
        Enumeration<InetAddress> ias = ni.getInetAddresses();
        while( ias.hasMoreElements() ) {
          ips.add(ias.nextElement());
        }
      }
    } catch( SocketException e ) {
    }

    InetAddress local = null;   // My final choice

    // Check for an "-ip xxxx" option and accept a valid user choice; required
    // if there are multiple valid IP addresses.
    InetAddress arg = null;
    if (H2O.OPT_ARGS.ip != null) {
      try{  arg = InetAddress.getByName(H2O.OPT_ARGS.ip);
      } catch( UnknownHostException e ){ Log.die(e.toString()); }
      if( !(arg instanceof Inet4Address) ) Log.die("Only IP4 addresses allowed.");
      for( InetAddress ip : ips ){ // Do a check to make sure the given IP
        if( ip.equals(arg) ){  // address refers can be found here
          local = arg; // Found it, so its a valid user-specified argument
          break;
        }
      }
      if( local == null ) Log.die("IP address not found on this machine");
    } else {
        // No user-specified IP address.  Attempt auto-discovery.  Roll through
        // all the network choices on looking for a single Inet4.  Complain about
        // them ALL if I see multiple valid addresses - the user must pick.
        InetAddress first = null;   // A first one
        for( InetAddress ip : ips ) { // Do a check to make sure the given IP address refers can be found here
          if( ip instanceof Inet4Address &&
              !ip.isLoopbackAddress() &&
              !ip.isLinkLocalAddress() ) {
            if( first == null ) local = first = ip; // Found a 1st valid address
            else {                  // Else found multiple addresses
              if( local == first ) {
                System.err.println("Found multiple valid IP4 addresses - pick one and rerun with the -ip option");
                System.err.println("  -ip "+first);
                first = ip;
              }
              System.err.println("  -ip "+ip);
            }
          }
        }
        if( local != first ) Log.die("local!=first");
    }

    // The above fails with no network connection, in that case go for a truly
    // local host.
    if( local == null ) {
      try {
        // set default ip address to be 127.0.0.1 /localhost
        local = InetAddress.getByName("127.0.0.1");
      } catch( UnknownHostException e ) {
        throw new Error(e);
      }
    }
    return local;
  }

  public final boolean is_cloud_member(H2O cloud) {
    return
      get_cloud_id_lo() == cloud._id.getLeastSignificantBits() &&
      get_cloud_id_hi() == cloud._id. getMostSignificantBits() &&
      cloud._memset.contains(this);
  }

  // Thin wrappers to send UDP packets to this H2ONode
  int send( byte[] buf, int len ) { return MultiCast.singlecast(this,buf,len); }
  void send( DatagramPacket p, int len ) { send(p.getData(),len); }

  // Happy printable string
  public String toString()  { return _key.toString (); }

  // index of this node in the current cloud... can change at the next cloud.
  public int index() { return H2O.CLOUD.nidx(this); }

  // Pick the lowest InetAddress
  public int compareTo( Object x ) {
    int src = 0;
    int dst = 0;
    if( x == null ) return -1;   // Always before null
    H2ONode h2o = (H2ONode)x;
    if( h2o == this ) return 0;
    byte[] self = this._key._inet.getAddress();
    byte[] othr = h2o ._key._inet.getAddress();
    // IP4 addresses before IP6
    if( self.length < othr.length ) return -1;
    if( self.length > othr.length ) return  1;
    for( int i=0; i<self.length; i++ ) {
      src = self[i] & 0xFF;
      dst = othr[i] & 0xFF;
      if( src != dst )
        return src-dst;
    }
    int res = this._key._port - h2o ._key._port;
    assert res != 0;            // Intern'g should prevent equal Inet+ports
    return res;
  }

  // ---------------
  // The Work-In-Progress list.  Each item is a UDP packet's worth of work.
  // When the 1st byte changes to an ACK, then it's Completed work instead
  // work-in-progress.  Completed work can be short-circuit replied-to by
  // resending this packet back.  Work that we're sure the this Node has seen
  // the reply to can be removed.
  private NonBlockingHashMapLong<DatagramPacket> WORK = new NonBlockingHashMapLong();

  // Record this packet (Node,task#) as being seen already, or report back the
  // prior version of this same task#.
  DatagramPacket putIfAbsent( DatagramPacket p ) {
    int tnum = UDP.get_task(p.getData());
    return WORK.putIfAbsent(tnum,p);
  }
  // Stop tracking a remote task, because we got an ACKACK
  void remove_task_tracking( long tnum ) {
    WORK.remove(tnum);
    synchronized( WORK ) { WORK.notifyAll(); } // wake up blocked pending gets
  }

  // This Node rebooted recently; we can quit tracking prior work history
  void rebooted() {
    WORK.clear();
    synchronized( WORK ) { WORK.notifyAll(); } // wake up blocked pending gets
  }

  // Block this thread until all pending gets from here to 'this' H2ONode are
  // served.  We've just changed a K/V mapping, and we have pending invalidates.
  // Remote H2ONodes need to either get the old value plus the invalidate, or
  // the new Value... but not the invalidate THEN the old Value - which will
  // leave them with a stale old Value.
  // We can be conservative here, and block for any set of tasks which include
  // all possible TGKs of the same Value.
  public void block_pending_gets( Value val ) {
    // Assert mem_replicas is locked down - and thus no new TGKs will appear on
    // this Value needing to be blocked here.
    assert val.mem_replicas() == -1; // Already locked down mem_replicas
    NonBlockingHashMapLong.IteratorLong ii = (NonBlockingHashMapLong.IteratorLong)WORK.keys();
    while( ii.hasNext() ) {
      final int task = (int)ii.nextLong();
      DatagramPacket p = WORK.get(task);
      if( p==null ) continue;   // Already removed
      byte[] buf = p.getData();
      int first_byte = UDP.get_ctrl(buf);
      assert first_byte != 0xab; // did not receive a clobbered packet?
      if( first_byte != UDP.udp.getkey.ordinal() && first_byte != UDP.udp.ack.ordinal() )
        continue;               // This cannot be a TGK, or it's ACK.
      // Clone a private copy: original 'p' gets freed at any moment in time.
      // As long as the task is still a key in the WORK set, then the packet
      // has not been freed & recycled so the clone is good.
      buf = buf.clone();
      if( WORK.get(task)!=p ) continue;

      // Here I have either a pending Get (possibly of an unrelated Key) or an
      // ACK of a Get, either of which might be for the same Key as the
      // invalidate.  Be conservative & block for it.
      try { ForkJoinPool.managedBlock(new FJB(buf)); } catch( InterruptedException e ) { }
    }
  }
  private class FJB implements ForkJoinPool.ManagedBlocker {
    final byte[] _buf;
    final int _t;
    private FJB(byte[] buf) { _buf=buf; _t = UDP.get_task(buf); }
    // Return true if blocking is unnecessary, which is true if the Task is missing
    public boolean isReleasable() {  return !WORK.containsKey(_t);  }
    // Possibly blocks the current thread.  Returns true if isReleasable would
    // return true.  Used by the FJ Pool management to spawn threads to prevent
    // deadlock is otherwise all threads would block on waits.
    public boolean block() {
      DatagramPacket p = new DatagramPacket(_buf,_buf.length);
      synchronized( WORK ) {
        while( !isReleasable() ) { // While this task is stll pending
          // Sometimes an ACKACK gets lost, but ACKS can fearlessly be resent
          // and we'll wait for an ACKACK.
          if( UDP.get_ctrl(_buf) == UDP.udp.ack.ordinal() )
            send(p,_buf.length);
          // Wait for the ACKACK to clear the WORK queue
          try { WORK.wait(1000); } catch( InterruptedException e ) { }
        }
      }
      return true;
    }
  }

  // ---------------

  // Unify multiple Key/Value fetches for the same Key from the same Node at
  // the "same time".  Large key fetches are slow, and we'll get multiple
  // requests close in time.  Batch them up.
  public static final ConcurrentHashMap<Key,TaskGetKey> TGKS = new ConcurrentHashMap();

  // ---------------
  // Build a 'heartbeat packet' used to hold a H2ONodes' latest health
  // measurements.  This is both a wire-line protocol and a directly readable
  // buffer.  It is multi-cast via the HeartBeatThread and read via the UDP
  // reading thread

  // Buffer size and offset()s, in bytes.
  public static enum hb {
    udp_enum(1),                // Packet type
    port(2),                    // Sending node port #
    cloud_id(16),               // Unique identifier for this Cloud
    cloud_md5(16),              // Unique identifier for this Cloud
    num_cpus(2),                // Number of CPUs for this Node, limit of 65535
    free_mem(3),                // Free memory in M (goes up and down with GC)
    tot_mem (3),                // Total memory in M (should track virtual mem?)
    max_mem (3),                // Max memory in M (max mem limit for JVM)
    keys(4),                    // Number of LOCAL keys in this node, cached or homed
    valsz(4),                   // Sum of value bytes used, in M
    free_disk(4),               // Free disk (internally stored in megabyte precision)
    max_disk(4),                // Disk size (internally stored in megabyte precision)
    cpu_util(2),                // CPU utilization
    cpu_load_1(2),              // CPU load over last 1 minute
    cpu_load_5(2),              // CPU load over last 5 minutes
    cpu_load_15(2),             // CPU load over last 15 minutes
    rpcs(2),                    // Outstanding DFutureTasks
    fjthrds_hi(2),              // Number of threads (not all are runnable)
    fjthrds_lo(2),              // Number of threads (not all are runnable)
    fjqueue_hi(2),              // Number of elements in FJ work queue
    fjqueue_lo(2),              // Number of elements in FJ work queue
    tcps_active(2),             // Threads trying do a TCP send
    node_type(1),               // Node type - used by HDFS to distinguish
                                // between data and name nodes
    total_in_conn(4),           // Total number of IN connections
    total_out_conn(4),          // Total number of OUT connections
    tcp_in_conn(4),             // Total number of TCP IN connections
    tcp_out_conn(4),            // Total number of TCP OUT connections
    udp_in_conn(4),             // Total number of UDP IN "connections" (i.e, opened server UDP socket)
    udp_out_conn(4),            // Total number of UDP OUT "connections" (i.e, opened client UDP socket)
    total_packets_recv(8),      // Total packets received
    total_packets_sent(8),      // Total packets sent
    total_bytes_recv(8),        // Total bytes received
    total_bytes_sent(8),        // Total bytes sent
    total_bytes_recv_rate(4),   // Incoming traffic rate
    total_bytes_sent_rate(4),   // Outgoing traffic rate
    tcp_packets_recv(8),        // TCP segments received
    tcp_packets_sent(8),        // TCP segments sent
    tcp_bytes_recv(8),          // TCP bytes received
    tcp_bytes_sent(8),          // TCP bytes sent
    udp_packets_recv(8),        // UDP packets received
    udp_packets_sent(8),        // UDP packets sent
    udp_bytes_recv(8),          // UDP packets received
    udp_bytes_sent(8),          // UDP packets sent
    max(0);
    ;
    private final int size;
    private int off;
    hb( int size ) {
      this.size = size;
      this.off  = -1;
    }

    public int offset() {
      if( off >= 0 ) return off;
      int offset = 0;
      hb[] values = values();
      for( int i = 0; i < ordinal(); ++i ) {
        offset += values[i].size;
      }
      return this.off = offset;
    }

  };

  // Getters and Setters
  public void set_health( byte[] buf ) {  System.arraycopy(buf,0,_health_buf,0,_health_buf.length);  }
  public void set_num_cpus (int  n) {     set_buf(hb.num_cpus.offset(),hb.num_cpus.size,n); }
  public void set_free_mem (long n) {     set_buf(hb.free_mem.offset(),hb.free_mem.size,n>>20); }
  public void set_tot_mem  (long n) {     set_buf(hb.tot_mem .offset(),hb.tot_mem .size,n>>20); }
  public void set_max_mem  (long n) {     set_buf(hb.max_mem .offset(),hb.max_mem .size,n>>20); }
  public void set_keys     (long n) {     set_buf(hb.keys    .offset(),hb.keys    .size,n    ); }
  public void set_valsz    (long n) {     set_buf(hb.valsz   .offset(),hb.valsz   .size,n>>20); }
  public void set_free_disk(long n) {     set_buf(hb.free_disk.offset(),hb.free_disk.size,n >> 20); }
  public void set_max_disk (long n) {     set_buf(hb.max_disk.offset(),hb.max_disk.size,n>>20); }
  public void set_cpu_util (double d) {
      if(d >= 0)
        set_buf(hb.cpu_util.offset(), hb.cpu_util.size,((long)(1000*d)) & 0xFFFF);
      else
        set_buf(hb.cpu_util.offset(), hb.cpu_util.size,0xFFFF);
  }
  public void set_cpu_load (double oneMinute, double fiveMinutes, double fifteenMinutes) {
    set_buf(hb.cpu_load_1 .offset(), hb.cpu_load_1 .size,
            oneMinute      >= 0 ? ((long)(1000*    oneMinute )) & 0xFFFF : 0xFFFF);
    set_buf(hb.cpu_load_5 .offset(), hb.cpu_load_5 .size,
            fiveMinutes    >= 0 ? ((long)(1000*   fiveMinutes)) & 0xFFFF : 0xFFFF);
    set_buf(hb.cpu_load_15.offset(), hb.cpu_load_15.size,
            fifteenMinutes >= 0 ? ((long)(1000*fifteenMinutes)) & 0xFFFF : 0xFFFF);
  }
  public void set_rpcs(int n)        { set_buf(hb.rpcs        .offset(),hb.rpcs        .size,n ); }
  public void set_fjthrds_hi(int qd) { set_buf(hb.fjthrds_hi  .offset(),hb.fjthrds_hi  .size,qd); }
  public void set_fjthrds_lo(int qd) { set_buf(hb.fjthrds_lo  .offset(),hb.fjthrds_lo  .size,qd); }
  public void set_fjqueue_hi(int qd) { set_buf(hb.fjqueue_hi  .offset(),hb.fjqueue_hi  .size,qd); }
  public void set_fjqueue_lo(int qd) { set_buf(hb.fjqueue_lo  .offset(),hb.fjqueue_lo  .size,qd); }
  public void set_tcps_active(int t) { set_buf(hb.tcps_active .offset(),hb.tcps_active .size,t ); }
  public void set_node_type(byte nt) { set_buf(hb.node_type   .offset(),hb.node_type   .size,nt); }
  public void set_total_in_conn(int n)  { set_buf(hb.total_in_conn.offset(), hb.total_in_conn.size, n); }
  public void set_total_out_conn(int n) { set_buf(hb.total_out_conn.offset(), hb.total_out_conn.size, n); }
  public void set_tcp_in_conn(int n)    { set_buf(hb.tcp_in_conn.offset(), hb.tcp_in_conn.size, n); }
  public void set_tcp_out_conn(int n)   { set_buf(hb.tcp_out_conn.offset(), hb.tcp_out_conn.size, n); }
  public void set_udp_in_conn(int n)    { set_buf(hb.udp_in_conn.offset(), hb.udp_in_conn.size, n); }
  public void set_udp_out_conn(int n)   { set_buf(hb.udp_out_conn.offset(), hb.udp_out_conn.size, n); }
  public void set_total_packets_recv(long n){ set_buf(hb.total_packets_recv.offset(), hb.total_packets_recv.size, n); }
  public void set_total_packets_sent(long n){ set_buf(hb.total_packets_sent.offset(), hb.total_packets_sent.size, n); }
  public void set_total_bytes_recv(long n){ set_buf(hb.total_bytes_recv.offset(), hb.total_bytes_recv.size, n); }
  public void set_total_bytes_sent(long n){ set_buf(hb.total_bytes_sent.offset(), hb.total_bytes_sent.size, n); }
  public void set_total_bytes_recv_rate(int n) { set_buf(hb.total_bytes_recv_rate.offset(), hb.total_bytes_recv_rate.size, n); }
  public void set_total_bytes_sent_rate(int n) { set_buf(hb.total_bytes_sent_rate.offset(), hb.total_bytes_sent_rate.size, n); }
  public void set_tcp_packets_recv(long n){ set_buf(hb.tcp_packets_recv.offset(), hb.tcp_packets_recv.size, n); }
  public void set_tcp_packets_sent(long n){ set_buf(hb.tcp_packets_sent.offset(), hb.tcp_packets_sent.size, n); }
  public void set_tcp_bytes_recv(long n){ set_buf(hb.tcp_bytes_recv.offset(), hb.tcp_bytes_recv.size, n); }
  public void set_tcp_bytes_sent(long n){ set_buf(hb.tcp_bytes_sent.offset(), hb.tcp_bytes_sent.size, n); }
  public void set_udp_packets_recv(long n) { set_buf(hb.udp_packets_recv.offset(), hb.udp_packets_recv.size, n); }
  public void set_udp_packets_sent(long n) { set_buf(hb.udp_packets_sent.offset(), hb.udp_packets_sent.size, n); }
  public void set_udp_bytes_recv(long n) { set_buf(hb.udp_bytes_recv.offset(), hb.udp_bytes_recv.size, n); }
  public void set_udp_bytes_sent(long n) { set_buf(hb.udp_bytes_sent.offset(), hb.udp_bytes_sent.size, n); }

  public int  get_num_cpus () {return (int)get_buf(hb.num_cpus.offset(),hb.num_cpus.size  ); }
  public long get_free_mem () {return      get_buf(hb.free_mem.offset(),hb.free_mem.size  )<<20; }
  public long get_tot_mem  () {return      get_buf(hb.tot_mem .offset(),hb.tot_mem .size  )<<20; }
  public long get_max_mem  () {return      get_buf(hb.max_mem .offset(),hb.max_mem .size  )<<20; }
  public long get_keys     () {return      get_buf(hb.keys    .offset(),hb.keys    .size  );     }
  public long get_valsz    () {return      get_buf(hb.valsz   .offset(),hb.valsz   .size  )<<20; }
  public long get_free_disk() {return      get_buf(hb.free_disk.offset(),hb.free_disk.size) << 20; }
  public long get_max_disk () {return      get_buf(hb.max_disk.offset(),hb.max_disk.size) << 20; }
  public double [] get_cpu_load () {
    double [] result = {-1.0,-1.0,-1.0};
    long oneM     = get_buf(hb.cpu_load_1 .offset(), hb.cpu_load_1 .size);
    long fiveM    = get_buf(hb.cpu_load_5 .offset(), hb.cpu_load_5 .size);
    long fifteenM = get_buf(hb.cpu_load_15.offset(), hb.cpu_load_15.size);
    if( oneM     != 0xFFFFL) result[0] = oneM     / 1000.0;
    if( fiveM    != 0xFFFFL) result[1] = fiveM    / 1000.0;
    if( fifteenM != 0xFFFFL) result[2] = fifteenM / 1000.0;
    return result;
  }
  public double get_cpu_util () {
    long n = get_buf(hb.cpu_util.offset(),hb.cpu_util.size);
    return n != 0xFFFFL ? n/1000.0 : -1.0;
  }
  public int get_rpcs()       { return (int)get_buf(hb.rpcs.offset(), hb.rpcs.size); }
  public int get_fjthrds_hi() { return (int)get_buf(hb.fjthrds_hi.offset(), hb.fjthrds_hi.size); }
  public int get_fjthrds_lo() { return (int)get_buf(hb.fjthrds_lo.offset(), hb.fjthrds_lo.size); }
  public int get_fjqueue_hi() { return (int)get_buf(hb.fjqueue_hi.offset(), hb.fjqueue_hi.size); }
  public int get_fjqueue_lo() { return (int)get_buf(hb.fjqueue_lo.offset(), hb.fjqueue_lo.size); }
  public int get_tcps_active() { return (int)get_buf(hb.tcps_active.offset(), hb.tcps_active.size); }
  public int get_total_in_conn()    { return (int)get_buf(hb.total_in_conn.offset(), hb.total_in_conn.size); }
  public int get_total_out_conn()   { return (int)get_buf(hb.total_out_conn.offset(), hb.total_out_conn.size); }
  public int get_tcp_in_conn()      { return (int)get_buf(hb.tcp_in_conn.offset(), hb.tcp_in_conn.size); }
  public int get_tcp_out_conn()     { return (int)get_buf(hb.tcp_out_conn.offset(), hb.tcp_out_conn.size); }
  public int get_udp_in_conn()      { return (int)get_buf(hb.udp_in_conn.offset(), hb.udp_in_conn.size); }
  public int get_udp_out_conn()     { return (int)get_buf(hb.udp_out_conn.offset(), hb.udp_out_conn.size); }
  public long get_total_packets_recv() { return get_buf(hb.total_packets_recv.offset(), hb.total_packets_recv.size); }
  public long get_total_packets_sent() { return get_buf(hb.total_packets_sent.offset(), hb.total_packets_sent.size); }
  public long get_total_bytes_recv() { return get_buf(hb.total_bytes_recv.offset(), hb.total_bytes_recv.size); }
  public long get_total_bytes_sent() { return get_buf(hb.total_bytes_sent.offset(), hb.total_bytes_sent.size); }
  public int  get_total_bytes_recv_rate() { return (int) get_buf(hb.total_bytes_recv_rate.offset(), hb.total_bytes_recv_rate.size); }
  public int  get_total_bytes_sent_rate() { return (int) get_buf(hb.total_bytes_sent_rate.offset(), hb.total_bytes_sent_rate.size); }
  public long get_tcp_packets_recv() { return get_buf(hb.tcp_packets_recv.offset(), hb.tcp_packets_recv.size); }
  public long get_tcp_packets_sent() { return get_buf(hb.tcp_packets_sent.offset(), hb.tcp_packets_sent.size); }
  public long get_tcp_bytes_recv()  { return get_buf(hb.tcp_bytes_recv.offset(), hb.tcp_bytes_recv.size); }
  public long get_tcp_bytes_sent()  { return get_buf(hb.tcp_bytes_sent.offset(), hb.tcp_bytes_sent.size); }
  public long get_udp_packets_recv(){ return get_buf(hb.udp_packets_recv.offset(), hb.udp_packets_recv.size); }
  public long get_udp_packets_sent(){ return get_buf(hb.udp_packets_sent.offset(), hb.udp_packets_sent.size); }
  public long get_udp_bytes_recv(){ return get_buf(hb.udp_bytes_recv.offset(), hb.udp_bytes_recv.size); }
  public long get_udp_bytes_sent(){ return get_buf(hb.udp_bytes_sent.offset(), hb.udp_bytes_sent.size); }

  public static final byte HDFS_NAMENODE = 'N';

  public byte get_node_type() { return (byte)get_buf(hb.node_type.offset(), hb.node_type.size); }
  public void set_cloud_id ( UUID id ) {
    set_buf(hb.cloud_id.offset()+0,8,id.getLeastSignificantBits());
    set_buf(hb.cloud_id.offset()+8,8,id. getMostSignificantBits());
  }
  public long get_cloud_id_lo() { return get_buf(hb.cloud_id.offset()+0,8);  }
  public long get_cloud_id_hi() { return get_buf(hb.cloud_id.offset()+8,8);  }

  public void set_cloud_md5 () {
    System.arraycopy(Boot._init._jarHash, 0, _health_buf, hb.cloud_md5.offset(), hb.cloud_md5.size);
  }
  public boolean check_cloud_md5 () {
    boolean match = true;
    for( int i = 0; i < hb.cloud_md5.size; ++i )
      match &= Boot._init._jarHash[i] == _health_buf[i + hb.cloud_md5.offset()];
    return match;
  }

  private long get_buf(int off, int size) {
    long sum=0;
    for( int i=0; i<size; i++ )
      sum |= (((long)(0xff&_health_buf[off+i]))<<(i<<3));
    return sum;
  }
  private void set_buf(int off, int size, long n) {
    assert size==8 || ((n>>>(size<<3))==0) || ((n>>(size<<3))==-1); // 'n' fits in 'size' bytes
    for( int i=0; i<size; i++ )
      _health_buf[off+i] = (byte)(n>>>(i<<3));
  }
}
