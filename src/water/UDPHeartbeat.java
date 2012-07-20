package water;

import java.net.DatagramPacket;

/**
 * A UDP Heartbeat packet.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPHeartbeat extends UDP {

  // Handle an incoming Heartbeat packet
  void call(DatagramPacket pack, H2ONode h2o) {
    h2o.set_health(pack.getData());        
    Paxos.do_heartbeat(h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Define the packet for a multicast announcement of Cloud membership,
  // and local Node health, published by any Node.
  static void build_and_multicast( H2O cloud ) {
    // Paxos.print_debug("send: heartbeat ",cloud._memset);
    H2O.SELF.set_cloud_id(cloud._id);
    H2ONode h2o = H2O.SELF;
    MultiCast.multicast(H2O.SELF._health_buf);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) { return ""; }
}
