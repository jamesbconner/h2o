package water;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;

import jsr166y.ForkJoinPool;

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
      putkey (false,new TaskPutKey .RemoteHandler()), // Put a Value for 1 Key
      rexec  (false,new TaskRemExec.RemoteHandler()), // Remote execution request
      atomic (false,new TaskRemExec.RemoteHandler()), // Remote transaction request
      ping   (true,new Ping.RemoteHandler()),
      pAck   (true,new Ping.UDPpingAck()),
      // This packet serves to obtain stdout/stderr/results from remote nodes
      log    (false, new RemoteLog()); //

    final UDP _udp;           // The Callable S.A.M. instance
    final boolean _paxos;     // Ignore (or not) packets from outside the Cloud
    udp( boolean paxos, UDP udp ) { _paxos = paxos; _udp = udp; }
    static public udp[] UDPS = values();
    // Default: most tasks go to the hi-priority queue
    ForkJoinPool pool() { return (this==rexec || this==atomic) ? H2O.FJP_NORM : H2O.FJP_HI; }
  };

  abstract void call(DatagramPacket pack, H2ONode h2o);

  void tcp_read_call( DataInputStream dis, H2ONode H2O ) throws IOException {
    throw new Error("Should override this");
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  static final char[] cs = new char[32];
  static char hex(long x) { x &= 0xf; return (char)(x+((x<10)?'0':('a'-10))); }
  public String print16( byte[] buf ) {
    for( int i=0; i<16; i++ ) {
      byte b = buf[i];
      cs[(i<<1)+0   ] = hex(b>>4);
      cs[(i<<1)+1   ] = hex(b   );
    }
    return new String(cs);
  }

  // Dispatch on the enum opcode and return a pretty string
  static private final byte[] pbuf = new byte[16];
  static public String printx16( long lo, long hi ) {
    set8(pbuf,0,lo);
    set8(pbuf,8,hi);
    return udp.UDPS[(int)(lo&0xFF)]._udp.print16(pbuf);
  }
  static private boolean port_check( byte[] buf, int port ) {
    int port1 = (0xFF&buf[1])+((0xFF&buf[2])<<8);
    if( port1 != 0 && port1 != 0xabab && port1 != port ) {
      System.err.println("good port 0x"+Integer.toHexString(port )+" port "+port );
      System.err.println("bad  port 0x"+Integer.toHexString(port1)+" port "+port1);
      throw new Error("broken ports");
      // return false;
    }
    return true;
  }

  // Set/get the 1st control byte
  public static int  get_ctrl( byte[] buf ) { return 0xFF&buf[0]; }
  public static void set_ctrl( byte[] buf, int c ) { buf[0] = (byte)c; }
  // Set/get the port in next 2 bytes
  public static int  get_port( byte[] buf ) { return (buf[1]&0xff) + ((buf[2]&0xff)<<8); }
  public static void set_port( byte[] buf, int port ) {
    // Assert buffer is clean (0), or from the Pool (0xab) or is recycled w/same port
    assert port_check(buf,port);
    buf[1] = (byte)port; buf[2] = (byte)(port>>8);
  }
  public static void clr_port( byte[] buf ) { // Reset the port assert
    buf[1]=0;
    buf[2]=0;
  }
  public static final int SZ_PORT = 1+2; // Offset past the control & port bytes
  // Set/get the task# in the next 4 bytes
  public static int  get_task( byte[] buf ) { return get4(buf,SZ_PORT); }
  public static void set_task( byte[] buf, int t ) { set4(buf,SZ_PORT,t); }
  public static final int SZ_TASK = SZ_PORT+4; // Offset past the control & port & task bytes

  public static int set1(byte[] buf, int off, int x) {
    buf[off++] = (byte) x;
    return off;
  }
  
  // Generic set/get
  public static int set2( byte[] buf, int off, int x ) {
    assert -32768 <= x && x <= 65535;
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
  public static int add2( byte[] buf, int off, int addend ) {
    int sum = (0xff&buf[off  ])   ;
    sum    += (0xff&buf[off+1])<<8;
    sum += addend;
    buf[off  ] = (byte) sum;
    buf[off+1] = (byte)(sum>>8);
    return sum;
  }
  public static int set3( byte[] buf, int off, int x ) {
    assert (-1<<24) <= x && x < (1<<24);
    for( int i=0; i<3; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 3;
  }
  public static int get3( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<3; i++ )
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
  public static int set4f( byte[] buf, int off, float f ) { return set4(buf,off,Float.floatToRawIntBits(f)); }
  public static int set8d( byte[] buf, int off, double d ) { return set8(buf,off,Double.doubleToRawLongBits(d)); }
  public static double get8d( byte[] buf, int off ) { return Double.longBitsToDouble(get8(buf,off)); }
  public static float  get4f( byte[] buf, int off ) { return Float . intBitsToFloat (get4(buf,off)); }

  public static int wire_len(  byte[]x ) { if( x==null ) return 4; return 4+(x.length<<0); }
  public static int wire_len( short[]x ) { if( x==null ) return 4; return 4+(x.length<<1); }
  public static int wire_len(  char[]x ) { if( x==null ) return 4; return 4+(x.length<<1); }
  public static int wire_len(   int[]x ) { if( x==null ) return 4; return 4+(x.length<<2); }
  public static int wire_len( float[]x ) { if( x==null ) return 4; return 4+(x.length<<2); }
  public static int wire_len(double[]x ) { if( x==null ) return 4; return 4+(x.length<<3); }
  public static int wire_len(  long[]x ) { if( x==null ) return 4; return 4+(x.length<<3); }

  public static int wire_len(  byte[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( byte[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( short[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( short[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( char[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( char[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( int[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( int[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( float[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( float[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( double[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( double[] b : x ) sum += wire_len(b);
    return sum;
  }
  public static int wire_len( long[][]x ) {
    if( x==null ) return 4;  int sum = 4;
    for( long[] b : x ) sum += wire_len(b);
    return sum;
  }
}
