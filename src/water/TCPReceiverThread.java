package water;
import java.net.ServerSocket;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
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
  @SuppressWarnings("resource")
  public void run() {
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    ServerSocketChannel sock = null, errsock = null;
    boolean saw_error = false;

    while( true ) {
      try {
        // Cleanup from any prior socket failures.  Rare unless we're really sick.
        if( errsock != null ) { // One time attempt a socket close
          final ServerSocketChannel tmp2 = errsock; errsock = null;
          tmp2.close();       // Could throw, but errsock cleared for next pass
        }
        if( saw_error ) Thread.sleep(1000); // prevent deny-of-service endless socket-creates
        saw_error = false;

        // ---
        // More common-case setup of a ServerSocket
        if( sock == null ) {
          sock = ServerSocketChannel.open();
          sock.socket().bind(H2O.SELF._key);
        }

        // Block for TCP connection and setup to read from it.
        AutoBuffer ab = new AutoBuffer(sock.accept());
        int ctrl = ab.getCtrl();
        ab.getPort();

        // Record the last time we heard from any given Node
        ab._h2o._last_heard_from = System.currentTimeMillis();

        // Hand off the TCP connection to the proper handler
        switch( UDP.udp.UDPS[ctrl] ) {
        case execlo:
        case exechi:   RPC.tcp_exec(ab); break;
        case ack:      RPC.tcp_ack (ab); break;
        case timeline: TimeLine.tcp_call(ab); break;
        default: throw new RuntimeException("Unknown Packet Type: " + ab.getCtrl());
        }

      } catch( Exception e ) {
        // On any error from anybody, close all sockets & re-open
        System.err.println("IO error on TCP port "+H2O.UDP_PORT+": "+e);
        e.printStackTrace();
        saw_error = true;
        errsock = sock ;  sock = null; // Signal error recovery on the next loop
      }
    }
  }
}
