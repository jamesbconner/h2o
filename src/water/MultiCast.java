package water;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

/**
 * MultiCast Writing Helper class.
 * 
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class MultiCast {
  static MulticastSocket sock = null;
  static DatagramPacket DPack = new DatagramPacket(new byte[0],0);
  // The assumed max UDP packetsize
  public static final int MTU = 1500-8/*UDP packet header size*/;

  // Write 'buf' out to the default H2O port as a single packet, using the
  // given InetAddress - which is sometimes a multicast and sometimes a single
  // target within the subnet.  sync'd: called from HeartBeat but also from the
  // Paxos driving threads.  Errors silently throw the packet away.  It's UDP,
  // so packet loss is expected.  Always returns a 0 (for handy flow-coding).
  synchronized private static int send( InetAddress ip, int port, byte[] buf, int off, int len ) {
    assert (0xFF&buf[0]) != 0xab; // did not receive a clobbered packet?
    try {
      if( sock == null ) {
        sock = new MulticastSocket();
        // Allow multicast traffic to go across subnets 
        sock.setTimeToLive(127);
      }
      // Setup for a send
      DPack.setAddress(ip);
      DPack.setPort(port);
      DPack.setData(buf,off,len);
      TimeLine.record_send(DPack);
      sock.send(DPack);
      System.out.println(((ip==H2O.CLOUD_MULTICAST_GROUP)?"multi":"single")+"cast send on DPack port "+DPack.getPort()+" and sock port "+sock.getPort());
      
    } catch( Exception e ) {
      // On any error from anybody, close all sockets & re-open
      System.err.println("UDP send on "+DPack.getAddress()+":"+DPack.getPort()+" got error "+e);
      // --- Cleanup any failed prior socket attempts
      if( sock != null ) {
        try {
          final DatagramSocket tmp = sock; sock = null;
          tmp.close();
        } catch( Exception e2 ) {
          System.err.println("Close on "+DPack.getAddress()+" got error "+e + " and "+ e2);
        }
      }
    }
    return 0;
  }


  // Write 'buf' out to the default H2O multicast port as a single packet.
  static int multicast( byte[] buf ) { return multicast(buf,0,buf.length); }
  static int multicast( byte[] buf, int off, int len ) {  
    if( H2O.MULTICAST_ENABLED ) {
      return send(H2O.CLOUD_MULTICAST_GROUP,H2O.CLOUD_MULTICAST_PORT,buf,off,len);
    } else {
      // Hideous O(n) algorithm for broadcast
      for( H2ONode h2o : H2O.NODES )
        send(h2o._key._inet,h2o._key._port,buf,off,len);
    }
    return 0;
  }

  static int singlecast( H2ONode h2o, byte[] buf, int len ) {
    assert H2O.SELF != h2o;   // Hey!  Pointless to send to self!!!
    return send(h2o._key._inet,h2o._key._port,buf,0,len);
  }
}
