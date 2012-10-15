package water;
import java.net.*;
import java.util.Enumeration;
import java.util.HashSet;

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
    assert UDP.get_ctrl(buf) != 0xab; // did not receive a clobbered packet?
    UDP.set_port(buf,H2O.UDP_PORT);   // Always jam in the sender (that's me!) port
    try {
      if( sock == null ) {
        sock = new MulticastSocket();
        // Allow multicast traffic to go across subnets
        sock.setTimeToLive(2);
        if( H2O.CLOUD_MULTICAST_IF != null )
          sock.setNetworkInterface(H2O.CLOUD_MULTICAST_IF);
      }
      // Setup for a send
      DPack.setAddress(ip);
      DPack.setPort(port);
      DPack.setData(buf,off,len);

      TimeLine.record_send(DPack);
      sock.send(DPack);
      assert UDP.get_port(buf) == H2O.UDP_PORT; // Detect racey packet-port-hacking
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
    if( H2O.STATIC_H2OS == null ) {
      return send(H2O.CLOUD_MULTICAST_GROUP,H2O.CLOUD_MULTICAST_PORT,buf,off,len);
    } else {
      // The multicast simulation is little bit tricky. To achieve union of all
      // specified nodes' flatfiles (via option -flatfile), the simulated
      // multicast has to send packets not only to nodes listed in the node's
      // flatfile (H2O.STATIC_H2OS), but also to all cloud members (they do not
      // need to be specified in THIS node's flatfile but can be part of cloud
      // due to another node's flatfile).
      //
      // Furthermore, the packet have to be send also to Paxos proposed members
      // to achieve correct functionality of Paxos.  Typical situation is when
      // this node receives a Paxos heartbeat packet from a node which is not
      // listed in the node's flatfile -- it means that this node is listed in
      // another node's flatfile (and wants to create a cloud).  Hence, to
      // allow cloud creation, this node has to reply.
      //
      // Typical example is:
      //    node A: flatfile (B)
      //    node B: flatfile (C), i.e., A -> (B), B-> (C), C -> (A)
      //    node C: flatfile (A)
      //    Cloud configuration: (A, B, C)
      //

      // Hideous O(n) algorithm for broadcast - avoid the memory allocation in
      // this method (since it is heavily used)
      HashSet<H2ONode> nodes = (HashSet<H2ONode>)H2O.STATIC_H2OS.clone();
      nodes.addAll(H2O.CLOUD._memset);
      nodes.addAll(Paxos.PROPOSED_MEMBERS);
      for( H2ONode h2o : nodes ) {
        send(h2o._key._inet,h2o._key._port,buf,off,len);
      }
    }
    return 0;
  }

  static int singlecast( H2ONode h2o, byte[] buf, int len ) {
    assert H2O.SELF != h2o;   // Hey!  Pointless to send to self!!!
    return send(h2o._key._inet,h2o._key._port,buf,0,len);
  }

  static int singlecast( H2ONode[] nodes, byte[] buf) {
    return singlecast(nodes, buf, buf.length);
  }

  static int singlecast( H2ONode[] nodes, byte[] buf, int len) {
    for (H2ONode node : nodes) {
      send(node._key._inet,node._key._port,buf,0,len);
    }

    return 0;
  }
}
