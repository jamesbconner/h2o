package water;
import java.net.DatagramPacket;

/**
 * A Paxos packet: an AcceptRequest packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosAccept extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    Paxos.do_accept(pack.getData(),h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Build an AcceptRequest packet.  It is our PROPOSAL with the 8 bytes of
  // Accept number set, plus the Value (wireline member protocol)
  static void build_and_multicast( byte[] buf ) {
    set_ctrl(buf,UDP.udp.paxos_accept.ordinal());
    MultiCast.multicast(buf);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) {
    int udp     = get_ctrl(buf);
    int port    = get_port(buf);
    int off     = UDP.SZ_PORT;
    long promise= get8(buf,off); off += 8;
    int old_pro = get4(buf,off); off += 4;
    return "old proposal="+old_pro+" old promise="+promise;
  }
}
