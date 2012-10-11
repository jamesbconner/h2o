package water;
import java.net.DatagramPacket;

/**
 * A Paxos packet: an Accepted packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosAccepted extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    Paxos.do_accepted(pack.getData(), h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Build an Accepted packet.  It has the membership, plus proposal
  static void build_and_multicast( byte[] buf ) {
    set_ctrl(buf,UDP.udp.paxos_accepted.ordinal());
    MultiCast.multicast(buf);
    Paxos.do_accepted(buf, H2O.SELF);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) {
    int off     = SZ_PORT;
    long promise= get8(buf,off); off += 8;
    int old_pro = get4(buf,off); off += 4;
    return "old proposal="+old_pro+" old promise="+promise;
  }
}
