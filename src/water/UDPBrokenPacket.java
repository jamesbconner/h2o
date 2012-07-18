package water;
import java.net.DatagramPacket;

/**
 * An unexpected UDP packet
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPBrokenPacket extends UDP {
  void call(DatagramPacket pack, H2ONode h2o) {
    throw new Error("I really should complain more about this broken packet "+pack);
  }
}

