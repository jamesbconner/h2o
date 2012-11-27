package water;
import java.net.DatagramPacket;

/**
 * A Paxos packet: a Nack packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class UDPPaxosNack extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    if(h2o.has_cloud_md5()) Paxos.do_nack(pack.getData(), h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Build an Nack packet.  It only has the proposal to nack
  static byte[] BUF = new byte[16];
  static void build_and_multicast( long proposal, H2ONode proposer ) {
    BUF[0] = (byte)UDP.udp.paxos_nack.ordinal();
    Paxos.set_promise(BUF, proposal);
    proposer.send(BUF,BUF.length);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) {
    int off     = SZ_PORT;
    long promise= get8(buf,off); off += 8;
    int old_pro = get4(buf,off); off += 4;
    return "old proposal="+old_pro+" old promise="+promise;
  }
}
