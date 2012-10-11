package water;
import java.net.DatagramPacket;

/**
 * A Paxos packet: a Promise
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPPaxosPromise extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    byte[] buf = pack.getData();
    // The received promise was sent in the proposal field!  (Because the
    // sender promises the proposal HE received)
    Paxos.do_promise(buf,h2o);
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  static int singlecast(byte[] buf, H2ONode leader) {
    UDP.set_ctrl(buf, udp.paxos_promise.ordinal()); // Set the UDP type byte
    if( leader == H2O.SELF) {
      return Paxos.do_promise(buf, leader);
    } else {
      return MultiCast.singlecast(leader, buf, buf.length);
    }
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) {
    int off     = SZ_PORT;
    long promise= get8(buf,off); off += 8;
    int old_pro = get4(buf,off); off += 4;
    return "old proposal="+old_pro+" old promise="+promise;
  }
}
