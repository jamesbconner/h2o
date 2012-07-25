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
    int off = UDP.SZ_TASK;            // Skip udp byte and port and task#
    off = _dt._jarkey.write(buf,off); // Write the Key for the ValueCode jar file

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
      UDP.clr_port(buf); // Re-using UDP packet, so side-step the port reset assert
      int off = UDP.SZ_TASK;          // Skip udp byte and port and task#
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
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      len = dt.wire_len();
      // missing check for reasonable wire-len results
      dt.write(buf,off);
      off += len;

      reply(p,off,h2o);
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      int off     = UDP.SZ_TASK; // Skip udp byte and port and task#
      byte rf     = buf[off++];            //  8
      int klen    = get2(buf,off); off+=2; // 10
      return "task# "+tasknum+" key["+klen+"]="+new String(buf,10,6);
    }
  }

  // Unpack the answer
  protected DRecursiveTask unpack( DatagramPacket p ) {
    // First SZ_TASK bytes have UDP type# and port# and task#.
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    _dt.read(buf,off);          // Read from UDP packet back into user task
    return _dt;
  }

}
