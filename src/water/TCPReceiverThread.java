package water;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Thread that looks for TCP Cloud requests.
 *
 * This thread just spins on reading TCP requests from other Nodes.
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TCPReceiverThread extends Thread {
  public TCPReceiverThread() { super("TCP Receiver"); }

  // How many threads would like to do TCP right now?
  public static final AtomicInteger TCPS_IN_PROGRESS = new AtomicInteger(0);

  // The Run Method.

  // Started by main() on a single thread, this code manages reading TCP requests
  public void run() {
    ServerSocket sock = null, errsock = null;
    boolean saw_error = false;
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);

    // Loop forever accepting Cloud Management requests
    while( true ) {
      Socket client=null;

      try {
        // ---
        // Cleanup from any prior socket failures.  Rare unless we're really sick.
        if( errsock != null ) { // One time attempt a socket close
          final ServerSocket tmp2 = errsock; errsock = null;
          tmp2.close();       // Could throw, but errsock cleared for next pass
        }
        if( saw_error ) Thread.sleep(1000); // prevent deny-of-service endless socket-creates
        saw_error = false;

        // ---
        // More common-case setup of a ServerSocket
        if( sock == null )
          sock = new ServerSocket(H2O.TCP_PORT);

        // Open a TCP connection
        client = sock.accept();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(client.getInputStream()));

        // Put out the control byte & sender port#
        int ctrl = dis.readByte()&0xFF;
        int port = dis.readShort()&0xFFFF;
        if(ctrl == UDP.udp.ping.ordinal()){
          FJPacket.call(ctrl,dis,null);
        } else {
          // Record the last time we heard from any given Node
          H2ONode hello = H2ONode.intern(client.getInetAddress(),port);
          hello._last_heard_from = System.currentTimeMillis();
          // Hand off the TCP connection to the proper handler
          FJPacket.call(ctrl,dis,hello);
        }
        // Write 1 byte of ACK
        OutputStream os = client.getOutputStream();
        os.write(99);           // Write 1 byte of ack

        client.close();

      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        System.err.println("IO error on port "+H2O.TCP_PORT+": "+e);
        e.printStackTrace();
        saw_error = true;
        errsock  = sock ;  sock  = null; // Signal error recovery on the next loop
      }
    }
  }

  // Utility functions to read & write arrays
  public static byte[] readByteAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    byte[] ary  = new byte[len];
    dis.readFully(ary);
    return ary;
  }
  public static short[] readShortAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    short[] ary  = new short[len];
    for( int i=0; i<len; i++ ) ary[i] = dis.readShort();
    return ary;
  }
  public static int[] readIntAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    int[] ary  = new int[len];
    for( int i=0; i<len; i++ ) ary[i] = dis.readInt();
    return ary;
  }
  public static float[] readFloatAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    float[] ary  = new float[len];
    for( int i=0; i<len; i++ ) ary[i] = dis.readFloat();
    return ary;
  }
  public static long[] readLongAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    long[] ary  = new long[len];
    for( int i=0; i<len; i++ ) ary[i] = dis.readLong();
    return ary;
  }
  public static double[] readDoubleAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    double[] ary  = new double[len];
    for( int i=0; i<len; i++ ) ary[i] = dis.readDouble();
    return ary;
  }
  public static byte[][] readByteByteAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    byte[][] ary  = new byte[len][];
    for( int i=0; i<len; i++ ) ary[i] = readByteAry(dis);
    return ary;
  }
  public static int[][] readIntIntAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    int[][] ary  = new int[len][];
    for( int i=0; i<len; i++ ) ary[i] = readIntAry(dis);
    return ary;
  }
  public static long[][] readLongLongAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    long[][] ary  = new long[len][];
    for( int i=0; i<len; i++ ) ary[i] = readLongAry(dis);
    return ary;
  }
  public static double[][] readDoubleDoubleAry( DataInputStream dis ) throws IOException {
    int len = dis.readInt(); if( len == -1 ) return null;
    double[][] ary  = new double[len][];
    for( int i=0; i<len; i++ ) ary[i] = readDoubleAry(dis);
    return ary;
  }
  public static String readStr( DataInputStream dis ) throws IOException {
    int len = dis.readChar(); if( len == 65535 ) return null;
    byte[] ary  = new byte[len];
    dis.readFully(ary);
    return new String(ary);
  }


  public static void writeAry( DataOutputStream dos, byte[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) dos.write(ary,0,ary.length);
  }
  public static void writeAry( DataOutputStream dos, short[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) dos.writeShort(ary[i]);
  }
  public static void writeAry( DataOutputStream dos, int[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) dos.writeInt(ary[i]);
  }
  public static void writeAry( DataOutputStream dos, float[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) dos.writeFloat(ary[i]);
  }
  public static void writeAry( DataOutputStream dos, long[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) dos.writeLong(ary[i]);
  }
  public static void writeAry( DataOutputStream dos, double[] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) dos.writeDouble(ary[i]);
  }
  public static void writeAry( DataOutputStream dos, byte[][] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) writeAry(dos,ary[i]);
  }
  public static void writeAry( DataOutputStream dos, int[][] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) writeAry(dos,ary[i]);
  }
  public static void writeAry( DataOutputStream dos, long[][] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) writeAry(dos,ary[i]);
  }
  public static void writeAry( DataOutputStream dos, double[][] ary ) throws IOException {
    dos.writeInt(ary==null?-1:ary.length);
    if( ary !=null ) for( int i=0; i<ary.length; i++ ) writeAry(dos,ary[i]);
  }
  public static void writeStr( DataOutputStream dos, String str ) throws IOException {
    if( str == null ) dos.writeChar(65535);
    else {
      assert str.length() < 65535;
      dos.writeChar(str.length());
      dos.writeBytes(str);
    }
  }
}
