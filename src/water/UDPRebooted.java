package water;
import java.net.DatagramPacket;

/**
 * A UDP Rebooted packet: this node recently rebooted
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPRebooted extends UDP {
  // Handle an incoming rebooted packet
  void call(DatagramPacket pack, H2ONode h2o) {
    if( h2o != null ) h2o.rebooted();
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  // Announce self-node reboot to the world
  static void build_and_multicast( ) {
    byte[] buf = new byte[16];
    buf[0] = (byte)UDP.udp.rebooted.ordinal();
    buf[SZ_PORT] = 1;    // This is a reboot announcement
    // Send it 3 times.  Obnoxious, but unlikely to be not heard
    MultiCast.multicast(buf);
    MultiCast.multicast(buf);
    MultiCast.multicast(buf);
  }

  // Announce self-node reboot to the world
  static public void global_kill( int x ) {
    byte[] buf = new byte[16];
    buf[0] = (byte)UDP.udp.rebooted.ordinal();
    buf[SZ_PORT] = (byte)x;     // This is a global-kill announcement
    // Send it 3 times.  Obnoxious, but unlikely to be not heard
    MultiCast.multicast(buf);
    MultiCast.multicast(buf);
    MultiCast.multicast(buf);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) { return ""; }
}
