package water;
import java.net.DatagramPacket;

/**
 * A Paxos proposal for membership
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosProposal extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    long proposal_num = get8(pack.getData(),3/*udp_enum*/);
    Paxos.do_proposal(proposal_num,h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Define the packet for making a Proposal.  The first 3 bytes are the UDP
  // packet type & port.  The next 8 bytes are proposal number.  This is a
  // packet of 11 bytes.  Single buffer, single threaded.
  static final byte[] PROPOSAL_BUF = new byte[16];
  static void build_and_multicast( final long proposal_num ) {
    set_ctrl(PROPOSAL_BUF,UDP.udp.paxos_proposal.ordinal());
    set8(PROPOSAL_BUF,3,proposal_num);
    MultiCast.multicast(PROPOSAL_BUF);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( long lo, long hi ) {
    long pnum = (lo>>>8) | ((hi&0xFF)<<56);
    return "p#"+Long.toHexString(pnum);
  }
}

