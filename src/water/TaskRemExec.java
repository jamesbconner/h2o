package water;
import java.net.DatagramPacket;

/**
 * A remote execution request
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskRemExec extends DFutureTask<RemoteTask> {

  final RemoteTask _dt;              // Task to send & execute remotely
  final Key _args;
  
  public TaskRemExec( H2ONode target, RemoteTask dt, Key args, Value val ) {
    super( target,UDP.udp.rexec );
    _dt = dt;
    _args = args;
    DKV.put(args,val);          // Publish the keyset for remote execution
    DKV.write_barrier();        // Block until all prior writes have completed
    resend();                   // Initial send after final fields set
  }

  // Pack classloader/class & the instance data into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;            // Skip udp byte and port and task#
    // Class loader first.  3 bytes of null for system loader.
    Class clazz = _dt.getClass();  // The exact classname to execute
    ClassLoader cl = clazz.getClassLoader();
    if( cl != null && false/*cl instanceof JarLoader*/ ) {
      throw new Error("unimplemented");
      //off = cl._jarkey.write(buf,off); // Write the Key for the ValueCode jar file
    } else {
      buf[off++] = 0; // zero RF
      off += UDP.set2(buf,off,0); // 2 bytes of jarkey length
    }
    // Class name now
    String sclazz = clazz.getName();  // The exact classname to execute
    off += UDP.set2(buf,off,sclazz.length());  // String length
    sclazz.getBytes(0,sclazz.length(),buf,off); // Dump the string also
    off += sclazz.length();
    // Then the args key
    off = _args.write(buf,off);
    // Then the instance data.
    if( _dt.wire_len()+off >= MultiCast.MTU ) {
      // Big object, switch to TCP style comms.  Or really, serialize the
      // object to the K/V store & send the key over - with the key home being
      // on the target machine.
      throw new Error("sending a big object?");
    } else {
      off = _dt.write(buf,off);
    }
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
      // Unpack the class loader first
      Key classloader_key;
      if( buf[off]==0 && UDP.get2(buf,off+1)==0 ) {
        classloader_key = null; // System loader
        off += 3;
      } else {
        classloader_key = Key.read(buf,off); // Key for the jar file - really a ClassLoader
        off += classloader_key.wire_len();
      }
      // Now the class string name
      int len = get2(buf,off);  off += 2; // Class string length
      String clazz = new String(buf,off,len);
      off += len;               // Skip string
      // Then the args key
      Key args = Key.read(buf,off);
      off += args.wire_len();
      // Make a remote instance of this dude
      RemoteTask dt = RemoteTask.make(classloader_key,clazz);
      // Fill in any fields
      if( dt != null ) {
        if( dt.wire_len()+off >= MultiCast.MTU ) {
          // Big object, switch to TCP style comms.  Or really, serialize the
          // object to the K/V store & send the key over - with the key home
          // being on the target machine.
          throw new Error("receiving a big object?");
        } else {
          dt.read(buf,off);
        }
      }

      // Now compute on it!
      dt.rexec(args);

      DKV.remove(args); // Cleanup the arg-passing
      // Send it back; UDP-sized results only please, for now
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      if( dt.wire_len()+off >= MultiCast.MTU ) {
        // Big object, switch to TCP style comms.  Or really, serialize the
        // object to the K/V store & send the key over - with the key home being
        // on the target machine.
        throw new Error("sending a big object?");
      } else {
        off = dt.write(buf,off);
      }

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
  protected RemoteTask unpack( DatagramPacket p ) {
    // First SZ_TASK bytes have UDP type# and port# and task#.
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#

    if( _dt.wire_len()+off >= MultiCast.MTU ) {
      // Big object, switch to TCP style comms.  Or really, serialize the
      // object to the K/V store & send the key over - with the key home being
      // on the target machine.
      throw new Error("sending a big object?");
    } else {
      _dt.read(buf,off);
    }
    return _dt;
  }

}
