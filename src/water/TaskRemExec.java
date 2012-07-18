package water;
import java.net.DatagramPacket;

/**
 * A remote execution request
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskRemExec extends DFutureTask<DRecursiveTask> {

  final DRecursiveTask _dt;     // Task to send & execute remotely
  
  public TaskRemExec( H2ONode target, DRecursiveTask dt ) {
    super( target,UDP.udp.rexec );
    _dt = dt;
    resend();                   // Initial send after final fields set
  }

  // Pack key+len into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = 5;
    off = _dt._jarkey.write(buf,off);  // Write the Key for the ValueCode jar file

    String clazz = _dt.getClass().getName();  // The exact classname to execute
    off += UDP.set2(buf,off,clazz.length());  // String length
    clazz.getBytes(0,clazz.length(),buf,off); // Dump the string also
    off += clazz.length();

    off = _dt._keykey.write(buf,off); // Write the key for the array of Keys in the DRT
    off += UDP.set2(buf,off,_dt.idx()); // Index position being worked on
    //System.out.println("Sending remote exec of idx "+_dt.idx()+" class "+clazz);
    return off;
  }

  // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
  // Node, not local.
  public static class RemoteHandler extends UDP {
    // Received a request for N keys.  Build & return the answer.
    void call(DatagramPacket p, H2ONode h2o) {
      // Unpack the incoming arguments
      byte[] buf = p.getData();
      int off = 5;              // Skip udp byte and task#
      Key jarkey = Key.read(buf,off); // Key for ValueCode, the jar file
      off += jarkey.wire_len();
      int len = get2(buf,off);  off += 2; // Class string length
      String clazz = new String(buf,off,len);
      off += len;               // Skip string
      Key keykey = Key.read(buf,off); // Key for array of Keys
      off += keykey.wire_len();
      int idx = get2(buf,off);  off += 2; // Index into the key array
      //System.out.println("Receiving remote exec of idx "+idx+" class "+clazz);
      DRecursiveTask dt = null;
      try {
        dt = ValueCode.exec_map(jarkey,clazz,keykey,idx);
        //System.out.print("Exec returned");
      } catch (Exception e) {
        System.out.println("ERROR executing "+e.toString());
      }

      // Send it back; UDP-sized results only please, for now
      off = 5;                  // Skip udp byte and task#
      len = dt.wire_len();
      // missing check for reasonable wire-len results
      dt.write(buf,off);
      off += len;

      reply(p,off,h2o);
      //System.out.println("Reply sent.");
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    private static final byte [] keyname = new byte[8];
    public String print16( long lo, long hi ) {
      int udp     = (int)(lo&0xFF)        ; lo>>>= 8;
      int tasknum = (int)lo               ; lo>>>=32;
      byte rf     = (byte)(lo&0xFF)       ; lo>>>= 8;
      short klen  = (short)(lo&0xFFFF)    ; lo>>>=16;
      set8(keyname,0,hi);
      return "task# "+tasknum+" key["+klen+"]="+new String(keyname);
    }
  }

  // Unpack the answer
  protected DRecursiveTask unpack( DatagramPacket p ) {
    // First 5 bytes have UDP type# and task#.
    byte[] buf = p.getData();
    int off = 5;
    _dt.read(buf,off);          // Read from UDP packet back into user task
    return _dt;
  }

}
