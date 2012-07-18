package water;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;

/**
 * Do Something with an incoming UDP packet
 *
 * Classic Single Abstract Method pattern.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class UDP {

  // Types of UDP packets I grok
  public static enum udp {
    bad(false,null), // Do not use the zero packet, too easy to make mistakes
      // Some Paxos-related and health-related packet types.  These packets are
      // all stateless, in that we do not need to send any replies back.
      heartbeat     ( true, new UDPHeartbeat()),
      paxos_proposal( true, new UDPPaxosProposal()),
      paxos_promise ( true, new UDPPaxosPromise()),
      paxos_nack    ( true, new UDPPaxosNack()),
      paxos_accept  ( true, new UDPPaxosAccept()),
      paxos_accepted( true, new UDPPaxosAccepted()),
      rebooted      ( true, new UDPRebooted()),  // This node has rebooted recently
      timeline      (false, new TimeLine()),     // Get timeline dumps from across the Cloud
      hazkey        (false, new UDPHazKeys()),   // Haz a Key (and want to know it is replicated)
      hazkeyack     (false, new UDPHazKeyAck()), // I also Haz this Key

      // All my *reliable* tasks (below), are sent to remote nodes who then ACK
      // back an answer.  To be reliable, I might send the TASK multiple times.
      // To get a reliable answer, the remote might send me multiple ACKs with
      // the same answer every time.  When does the remote know it can quit
      // tracking reply ACKs?  When it recieves an ACKACK.
      ackack(false,new UDPAckAck()),  // a generic ACKACK for a UDP async task
      ack   (false,new UDPAck   ()),  // a generic ACK    for a UDP async task

      // These packets all imply some sort of request/response handshake.
      // We'll hang on to these packets; filter out dup sends and auto-reply
      // identical result ACK packets.
      getkeys(false,new TaskGetKeys.RemoteHandler()), // Get a collection of Keys
      getkey (false,new TaskGetKey .RemoteHandler()), // Get a Value for 1 Key
      rexec  (false,new TaskRemExec.RemoteHandler()); // Remote execution request

    final UDP _udp;           // The Callable S.A.M. instance
    final boolean _paxos;     // Ignore (or not) packets from outside the Cloud
    udp( boolean paxos, UDP udp ) { _paxos = paxos; _udp = udp; }
    static public udp[] UDPS = values();
  };

  abstract void call(DatagramPacket pack, H2ONode h2o);

  void tcp_read_call( DataInputStream dis, H2ONode H2O ) throws IOException {
    throw new Error("Should override this");
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  static final char[] cs = new char[32];
  static char hex(long x) { x &= 0xf; return (char)(x+((x<10)?'0':('a'-10))); }
  public String print16( long lo, long hi ) {
    for( int i=0; i<8; i++ ) {
      cs[(i<<1)+0   ] = hex((lo>>(i<<3))>>4);
      cs[(i<<1)+1   ] = hex((lo>>(i<<3))   );
    }
    for( int i=0; i<8; i++ ) {
      cs[(i<<1)+0+16] = hex((hi>>(i<<3))>>4);
      cs[(i<<1)+1+16] = hex((hi>>(i<<3))   );
    }
    return new String(cs);
  }

  // Dispatch on the enum opcode and return a pretty string
  static public String printx16( long lo, long hi ) {
    return udp.UDPS[(int)(lo&0xFF)]._udp.print16(lo,hi);
  }


  public static int set2( byte[] buf, int off, int x ) {
    for( int i=0; i<2; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 2;
  }
  public static int get2( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<2; i++ )
      sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }
  public static int set4( byte[] buf, int off, int x ) {
    for( int i=0; i<4; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 4;
  }
  public static int get4( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<4; i++ )
      sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }

  public static int set8( byte[] buf, int off, long x ) {
    for( int i=0; i<8; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 8;
  }
  public static long get8( byte[] buf, int off ) {
    long sum=0;
    for( int i=0; i<8; i++ )
      sum |= ((long)(0xff&buf[off+i]))<<(i<<3);
    return sum;
  }
  public static int set8( byte[] buf, int off, double d ) { return set8(buf,off,Double.doubleToRawLongBits(d)); }
  public static double get8d( byte[] buf, int off ) { return Double.longBitsToDouble(get8(buf,off)); }
}

