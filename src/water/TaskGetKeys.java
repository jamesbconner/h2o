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
  // First 5 bytes of UDP type & task num, then offset/length
  public TaskGetKeys( H2ONode target, int skip, int len ) {
    super( target,UDP.udp.getkeys );
    _skip = skip;               // Set final fields
    _len  = len;
    resend();                   // Initial send after final fields set
  }

  // Pack off/len into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = 5;                // Skip udp & task#
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
      int off = 5;                // Skip udp & task#
      final int skip = get4(buf,off);
      final int len  = get4(buf,off+4);

      // First 5 bytes have UDP type# and task#.  The number of responses I am
      // returning in the next byte.  This limits me to 255 responses.
      off = 6;
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
      buf[5] = (byte)(num-skip); // Set number of keys returned

      // Send it back
      reply(p,off,h2o);
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( long lo, long hi ) {
      int udp     = (int)(lo&0xFF)        ; lo>>>= 8;
      int tasknum = (int)lo               ; lo>>>=32;
      int skip = (int)(lo|((hi&0xFF)<<24)); hi>>>= 8;
      int len     = (int)hi               ; hi>>>=32;
      return "task# "+tasknum+" skip "+skip+" keys, return next "+len+" keys";
    }
  }

  // Unpack the answer
  protected Key[] unpack( DatagramPacket p ) {
    byte[] buf = p.getData();
    // First 5 bytes have UDP type# and task#.  Result key count in the next
    // byte.  Keys jammed in after that.
    int numkeys = buf[5];
    Key[] keys = new Key[numkeys];
    int off = 6;
    for( int i=0; i<numkeys; i++ ) {
      keys[i] = Key.read(buf,off);
      off += keys[i].wire_len();
    }
    return keys;
  }
}

