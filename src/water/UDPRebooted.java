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
    locked,
    mismatch;

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
      String m;
      switch( T.values()[type] ) {
      case error:    m = "Error leading to a cloud kill"              ; break;
      case shutdown: m = "Orderly shutdown command"                   ; break;
      case locked:   m = "Killed joining a locked cloud"              ; break;
      case mismatch: m = "Killed joining a cloud with a different jar"; break;
      default:       m = "Received kill "+type                        ; break;
      }
      System.err.println("[h2o] "+m+" from "+h2o);
      System.exit(-1);
    }
  }

  public String print16( byte[] buf ) { return ""; }
}
