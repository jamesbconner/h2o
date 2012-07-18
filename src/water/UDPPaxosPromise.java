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

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  // Since we are not recording the entire promise value, do not print it.
  public String print16( long lo, long hi ) { return ""; }
}
