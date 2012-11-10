package water;
import java.net.DatagramPacket;

/**
 * A UDP Rebooted packet: this node recently rebooted
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPRebooted extends UDP {
  public static enum T {
    none,
    reboot,
    shutdown,
    error,
    locked;

    public void singlecast(H2ONode target) {
      byte[] buf = make(); // Send it 3 times.  Obnoxious, but effective
      MultiCast.singlecast(target, buf, buf.length);
      MultiCast.singlecast(target, buf, buf.length);
      MultiCast.singlecast(target, buf, buf.length);
    }

    public void broadcast()  {
      byte[] buf = make(); // Send it 3 times.  Obnoxious, but effective
      MultiCast.multicast(buf);
      MultiCast.multicast(buf);
      MultiCast.multicast(buf);
    }

    private byte[] make() {
      assert this != none;
      byte[] buf = new byte[16];
      buf[0] = (byte)UDP.udp.rebooted.ordinal();
      buf[SZ_PORT] = (byte)ordinal();
      return buf;
    }
  }

  // Handle an incoming rebooted packet
  void call(DatagramPacket pack, H2ONode h2o) {
    if( h2o != null ) h2o.rebooted();
    // This is a stateless paxos-style packet; we must free it
    UDPReceiverThread.free_pack(pack);
  }

  public static void checkForSuicide(int first_byte, byte[] pbuf, H2ONode h2o) {
    if( first_byte != UDP.udp.rebooted.ordinal() ) return;
    int type = pbuf[UDP.SZ_PORT];
    if( type > 1 ) {
      switch( T.values()[type] ) {
      case error:    System.err.println("[h2o] Error leading to a cloud kill from "+h2o); break;
      case shutdown: System.err.println("[h2o] Orderly shutdown command from "     +h2o); break;
      case locked:   System.err.println("[h2o] Killed joining a locked cloud from "+h2o); break;
      default:       System.err.println("[h2o] Received kill "+type+" from "       +h2o); break;
      }
      System.exit(-1);
    }
  }

  public String print16( byte[] buf ) { return ""; }
}
