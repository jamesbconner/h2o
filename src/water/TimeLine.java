package water;
import java.io.*;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import sun.misc.Unsafe;
import water.nbhm.UtilUnsafe;

/**

 * Maintain a VERY efficient list of events in the system.  This must be VERY
 * cheap to call, as it will get called alot.  On demand, we can snapshot this
 * list gather all other lists from all other (responsive) Nodes, and build a
 * whole-Cloud timeline for dumping.
 *
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TimeLine extends UDP {
  private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();

  // The TimeLine buffer.

  // The TimeLine buffer is full of Events; each event has a timestamp and some
  // event bytes.  The buffer is a classic ring buffer; we toss away older
  // events.  We snapshot the buffer by replacing it with a fresh array.  The
  // index of the next free slot is kept in the 1st long of the array, and
  // there are MAX_EVENTS (a power of 2) more slots.

  // A TimeLine event is:
  // - Milliseconds since JVM boot; 4 bytes
  // - IP4 of send/recv
  // - Sys.Nano, 8 bytes -1 bit
  // - Nano low bit is 0 for send, 1 for recv
  // - 16 bytes of payload; 1st byte is a udp_type opcode, next 4 bytes are typically task#
  static final int MAX_EVENTS=1024; // Power-of-2, please
  static final int WORDS_PER_EVENT=4;
  static final long[] TIMELINE = new long[MAX_EVENTS*WORDS_PER_EVENT+1];

  static long JVM_BOOT_MSEC = System.currentTimeMillis();


  // Snapshot and return the current TIMELINE array
  public static long[] snapshot() { return TIMELINE.clone(); }

  // CAS access to the TIMELINE array
  private static final int _Lbase  = _unsafe.arrayBaseOffset(long[].class);
  private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);
  private static long rawIndex(long[] ary, int i) {
    assert i >= 0 && i < ary.length;
    return _Lbase + i * _Lscale;
  }
  private final static boolean CAS( long[] A, int idx, long old, long nnn ) {
    return _unsafe.compareAndSwapLong( A, rawIndex(A,idx), old, nnn );
  }
  // Return the next index into the TIMELINE array
  private final static int next_idx( long [] tl ) {
    // Spin until we can CAS-acquire a fresh index
    while( true ) {
      int oldidx = (int)tl[0];
      int newidx = (oldidx+1)&(MAX_EVENTS-1);
      if( CAS( tl, 0, oldidx, newidx ) )
        return oldidx;
    }
  }

  // Record 1 event, the first 16 bytes of this buffer.  This is expected to be
  // a high-volume multi-thread operation so needs to be fast.
  private static void record( DatagramPacket p, int sr ) {
    final long ms = System.currentTimeMillis(); // Read first, in case we're slow storing values
    final long ns = System.nanoTime();
    final long[] tl = TIMELINE; // Read once, in case the whole array shifts out from under us
    final int idx = next_idx(tl); // Next free index
    long deltams = ms-JVM_BOOT_MSEC;
    assert deltams < 0x0FFFFFFFFL; // No daily overflow
    int ip4 = get4(p.getAddress().getAddress(),0);
    tl[idx*WORDS_PER_EVENT+0+1] = (deltams)<<32 | (ip4&0x0FFFFFFFFL);
    tl[idx*WORDS_PER_EVENT+1+1] = (ns&~1)|sr;
    tl[idx*WORDS_PER_EVENT+2+1] = get8(p.getData(),0);
    tl[idx*WORDS_PER_EVENT+3+1] = get8(p.getData(),8);
  }
  public static void record_send( DatagramPacket p ) { record(p,0); }
  public static void record_recv( DatagramPacket p ) { record(p,1); }

  // Accessors, for TimeLines that come from all over the system
  public static int length( ) { return MAX_EVENTS; }
  // Internal array math so we can keep layout private
  private static int idx(long[] tl, int i ) { return (((int)tl[0]+i)&(MAX_EVENTS-1))*WORDS_PER_EVENT+1; }
  // That first long is complex: compressed CTM and IP4
  private static long x0( long[] tl, int idx ) { return tl[idx(tl,idx)+0]; }
  public static long ms( long[] tl, int idx ) { return (x0(tl,idx)>>>32)+JVM_BOOT_MSEC; }
  private static final byte _inet[] = new byte[4];
  public static InetAddress inet( long[] tl, int idx ) {
    set4(_inet,0,(int)x0(tl,idx));
    try { return InetAddress.getByAddress(_inet); }
    catch( UnknownHostException e ) { }
    return null;
  }
  // That 2nd long is nanosec, plus the low bit is send/recv
  public static long ns( long[] tl, int idx ) { return tl[idx(tl,idx)+1]; }
  // Returns zero for send, 1 for recv
  public static int send_recv( long[] tl, int idx ) { return (int)(ns(tl,idx)&1); }
  // 16 bytes of payload
  public static long l0( long[] tl, int idx ) { return tl[idx(tl,idx)+2]; }
  public static long l8( long[] tl, int idx ) { return tl[idx(tl,idx)+3]; }

  public static boolean isEmpty( long[] tl, int idx ) { return tl[idx(tl,idx)+0]==0; }

  // Take a system-wide snapshot.  Return an array, indexed by H2ONode _idx,
  // containing that Node's snapshot.  Try to get all the snapshots as close as
  // possible to the same point in time.
  static long[][] SNAPSHOT;
  static long TIME_LAST_SNAPSHOT = 1;
  static public H2O CLOUD;      // Cloud instance being snapshotted
  static public long[][] system_snapshot() {
    // Now spin-wait until we see all snapshots check in.
    // Be atomic about it.
    synchronized( TimeLine.class ) {
      // First see if we have a recent snapshot already.
      long now = System.currentTimeMillis();
      if( now - TIME_LAST_SNAPSHOT < 3*1000 )
        return SNAPSHOT;        // Use the recent snapshot

      // A new snapshot is being built?
      if( TIME_LAST_SNAPSHOT != 0 ) {
        TIME_LAST_SNAPSHOT = 0; // Only fire off the UDP packet once; flag it
        // Make a new empty snapshot
        CLOUD = H2O.CLOUD;
        SNAPSHOT = new long[CLOUD.size()][];
        // Broadcast a UDP packet, with the hopes of getting all SnapShots as close
        // as possible to the same point in time.
        byte[] buf = new byte[16];
        buf[0] = (byte)UDP.udp.timeline.ordinal();
        MultiCast.multicast(buf);
      }
      // Spin until all snapshots appear
      while( true ) {
        boolean done = true;
        for( int i=0; i<CLOUD._memary.length; i++ )
          if( SNAPSHOT[i] == null )
            done = false;
        if( done ) break;
        try { TimeLine.class.wait(); } catch( InterruptedException e ) {}
      }
      TIME_LAST_SNAPSHOT = System.currentTimeMillis();
      return SNAPSHOT;
    }
  }

  // Send our most recent timeline to the remote via TCP
  public void call( DatagramPacket p, H2ONode target ) {
    long[] a = snapshot();
    if( target == H2O.SELF ) {
      synchronized(TimeLine.class) {
        for( int i=0; i<CLOUD._memary.length; i++ )
          if( CLOUD._memary[i]==H2O.SELF )
            SNAPSHOT[i] = a;
        TimeLine.class.notify();
      }
      return; // No I/O needed for my own snapshot
    }
    try {
      TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(1);
      Socket sock = new Socket( p.getAddress(), target._key.tcp_port() );
      DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
      // Write out the initial operation
      dos.writeByte(UDP.udp.timeline.ordinal());
      dos.writeShort(H2O.UDP_PORT); // Our node identifier
      dos.writeInt(a.length);
      for( int i=0; i<a.length; i++ )
        dos.writeLong(a[i]);
      dos.flush();
      InputStream is = sock.getInputStream();
      int ack = is.read();      // Read 1 byte of ack
      sock.close();
      if( ack != 99 ) throw new IOException("missing tcp ack "+ack);
      TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(-1);
    } catch( IOException e ) {  // Failure?
      // Silently ignore failure, and the poor target does not get his dump.  I
      // am probably REALLY REALLY sick, which is why the dump is being asked for.
      TCPReceiverThread.TCPS_IN_PROGRESS.addAndGet(-1);
    }
  }

  // Receive a remote timeline
  void tcp_read_call( DataInputStream dis, H2ONode h2o ) throws IOException {
    int len = dis.readInt();
    long[] timeline = new long[len];
    for( int i=0; i<len; i++ )
      timeline[i] = dis.readLong();
    synchronized(TimeLine.class) {
      for( int i=0; i<CLOUD._memary.length; i++ )
        if( CLOUD._memary[i]==h2o )
          SNAPSHOT[i] = timeline;
      TimeLine.class.notify();
    }
  }
}

