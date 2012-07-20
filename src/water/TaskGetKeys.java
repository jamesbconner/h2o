package water;
import java.net.DatagramPacket;

/**
 * A remote request for N keys
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskGetKeys extends DFutureTask<Key[]> {

  final int _skip, _len;

  // Asking to send a request for N keys... not the answer!
  // Finish building the outbound UDP packet and send it.
  // First SZ_TASK bytes of UDP type & port & task num, then offset/length
  public TaskGetKeys( H2ONode target, int skip, int len ) {
    super( target,UDP.udp.getkeys );
    _skip = skip;               // Set final fields
    _len  = len;
    resend();                   // Initial send after final fields set
  }

  // Pack off/len into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    UDP.set4(buf,off+0,_skip);
    UDP.set4(buf,off+4,_len );
    return off+4+4;
  }

  // Handle the remote-side incoming UDP packet.
  // This is called on the REMOTE Node, not local.
  public static class RemoteHandler extends UDP {
    // Received a request for N keys.  Build & return the answer.
    void call(DatagramPacket p, H2ONode h2o) {
      // Unpack the incoming arguments
      byte[] buf = p.getData();
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      final int skip = get4(buf,off);
      final int len  = get4(buf,off+4);

      // First SZ_TASK bytes have UDP type# and port# and task#.  The number of
      // responses I am returning in the next byte.  This limits me to 255
      // responses.
      off += 1;
      int num = 0;
      for( Key key : H2O.keySet() ) {
        if( num < skip ) continue; // Skip the first so many Keys
        int off2 = key.write(buf,off);
        // Stop if we would use more than the MTU.
        // This limits me to 92 16-byte Keys for a 1500 MTU.
        if( off2 > MultiCast.MTU ) break;
        off = off2;
        num++;         // Wrote another key
        if( num >= skip+len ) break; // Have hit requested key count
      }
      buf[SZ_TASK] = (byte)(num-skip); // Set number of keys returned

      // Send it back
      reply(p,off,h2o);
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      int off     = UDP.SZ_TASK;           // Skip udp byte and port and task#
      int skip    = get4(buf,off); off+=4; // 11
      int len     = get4(buf,off); off+=4; // 15
      return "task# "+tasknum+" skip "+skip+" keys, return next "+len+" keys";
    }
  }

  // Unpack the answer
  protected Key[] unpack( DatagramPacket p ) {
    byte[] buf = p.getData();
    // First SZ_TASK bytes have UDP type# and port# and task#.  Result key
    // count in the next byte.  Keys jammed in after that.
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    int numkeys = buf[off++];
    Key[] keys = new Key[numkeys];
    for( int i=0; i<numkeys; i++ ) {
      keys[i] = Key.read(buf,off);
      off += keys[i].wire_len();
    }
    return keys;
  }
}

