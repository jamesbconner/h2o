package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import jsr166y.*;

/**
 * A remote execution request
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskRemExec<T extends RemoteTask> extends DFutureTask<T> {

  final T _dt;                  // Task to send & execute remotely
  final Key _args;
  final boolean _did_put;

  // With a Key+Value, do a Put on the Key & block for it - forcing the Value
  // to be available when remote execution starts.
  public TaskRemExec( H2ONode target, T dt, Key args, Value val ) {
    super( target,UDP.udp.rexec );
    _dt = dt;
    _args = args;
    _did_put = true;
    DKV.put(args,val);          // Publish the keyset for remote execution
    DKV.write_barrier();        // Block until all prior writes have completed
    resend();                   // Initial send after final fields set
  }

  // This version assumes a prior remote Value is already coherent
  public TaskRemExec( H2ONode target, T dt, Key args ) {
    super( target,UDP.udp.rexec );
    _dt = dt;
    _args = args;
    _did_put = false;
    resend();                   // Initial send after final fields set
  }

  // Pack classloader/class & the instance data into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    Class clazz = _dt.getClass();  // The exact classname to execute
    String sclazz = clazz.getName();  // The exact classname to execute
    // Then the instance data.
    int off = UDP.SZ_TASK             // Skip udp byte and port and task#
      + 1 // udp/tcp flag
      + 3 // jarkey
      + 2 // string len
      + sclazz.length()
      + _args.wire_len()
      + _dt.wire_len();
    if( off <= MultiCast.MTU ) {
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      buf[off++] = 0;           // Sending via UDP
      // Class loader first.  3 bytes of null for system loader.
      ClassLoader cl = clazz.getClassLoader();
      if( cl != null && false/*cl instanceof JarLoader*/ ) {
        throw new Error("unimplemented");
        //off = cl._jarkey.write(buf,off); // Write the Key for the ValueCode jar file
      } else {
        buf[off++] = 0; // zero RF
        off += UDP.set2(buf,off,0); // 2 bytes of jarkey length
      }
      // Class name now
      off += UDP.set2(buf,off,sclazz.length());  // String length
      sclazz.getBytes(0,sclazz.length(),buf,off); // Dump the string also
      off += sclazz.length();
      // Then the args key
      off = _args.write(buf,off);
      off = _dt.write(buf,off);
    } else {                    // Big object, switch to TCP style comms.
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      buf[off++] = 1;           // Sending via TCP
      tcp_send_pack(new Byte((byte)5/*setup remote call*/),sclazz,/*jarkey,*/_args,_dt);
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
      // Fill in any fields
      if( buf[off++] == 0 ) {   // Sent via UDP
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
        
        // Fill in remote values
        dt.read(buf,off);

        remexec(dt,args,p,h2o);
      } else {               // Else all the work is being done in the TCP thread
        // No "reply" here: the TCP thread will ship a reply.  Also, the packet
        // is not-yet-dead: its the eventual reply packet, and it is recorded
        // in the H2ONode task list.
      }

    }

    // Do the remote execution in a F/J thread & send a reply packet
    static void remexec( RemoteTask dt, Key args, DatagramPacket p, H2ONode h2o ) {
      // Now compute on it!
      dt.invoke(args);

      byte[] buf = p.getData();

      // Send it back
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      if( !dt.void_result() ) {
        if( dt.wire_len()+off+1 <= MultiCast.MTU ) {
          buf[off++] = 2;         // Result coming via UDP
          off = dt.write(buf,off); // Result
        } else {
          buf[off++] = 3;         // Result coming via TCP
          // Push the large result back *now* (no async pause) via TCP
          if( !tcp_send(h2o,UDP.udp.rexec,get_task(buf),new Byte((byte)6/*response from remote*/),dt) )
            return; // If the TCP failed... then so do we; no result; caller will retry
        }
      }
      reply(p,off,h2o);
    }

    // TCP large DRemoteTask RECEIVE of results.  Note that 'this' is NOT thed
    // TaskRemExec object that is hoping to get the received object, nor is the
    // current thread the TRE thread blocking for the object.  The current
    // thread is the TCP reader thread.
    void tcp_read_call( DataInputStream dis, final H2ONode h2o ) throws IOException {
      // Read all the parts
      int tnum = dis.readInt();
      int flag = dis.readByte(); // 1==setup, 2==response
      assert flag==5 || flag==6;

      if( flag==5 ) {           // Incoming TCP-style remote exec request?
        // Read clazz string
        int len = dis.readShort();
        byte[] bits = new byte[len];
        dis.readFully(bits);
        String clazz = new String(bits);
        Key classloader_key = null; // jarkey someday
        final Key args = Key.read(dis);
        // Make a remote instance of this dude
        final RemoteTask dt = RemoteTask.make(classloader_key,clazz);
        // Fill in remote values
        dt.read(dis);

        // Need the UDP packet because the finish-up code expects one.  Act "as
        // if" called from the UDP packet code, by making the exact UDP packet
        // we will be recieving (eventually).  The presence of this packet is used
        // to stop dup-actions on dup-sends.
        DatagramPacket p1 = UDPReceiverThread.get_pack(); // Get a fresh empty packet
        final byte[] buf = p1.getData();
        UDP.set_ctrl(buf,UDP.udp.rexec.ordinal());
        UDP.clr_port(buf);
        UDP.set_task(buf,tnum);
        buf[UDP.SZ_TASK] = 1;   // Just the "send by TCP" flag
        DatagramPacket p2 = h2o.putIfAbsent(p1);          // UDP race to insert pack
        if( p2 != null ) UDPReceiverThread.free_pack(p1); // UDP raced ahead of us?
        final DatagramPacket p = p2==null ? p1 : p2;

        // Here I want to execute on this, but not block for completion in the
        // TCP reader thread.  
        RecursiveTask rt = new RecursiveTask() {
            public Object compute() { remexec(dt,args,p,h2o); return null; }
          };
        H2O.FJP.execute(rt);
        // All done for the TCP thread!  Work continues in the FJ thread...

      } else {                  // Incoming TCP-style remote exec answer?
        // Get the TGK we're waiting on
        TaskRemExec tre = (TaskRemExec)TASKS.get(tnum);
        // Race with canceling a large Value fetch: Task is already dead.  Do not
        // bother reading from the TCP socket, just bail out & close socket.
        if( tre == null ) return;
        
        // Big Read of Big Results
        tre._dt.read(dis);
        // Here we have the result, and we're on the correct Node but wrong
        // Thread.  If we just return, the TCP reader thread will toss back a TCP
        // ack to the remote, the remote will UDP ACK the TaskRemExec back, and
        // back on the current Node but in the correct Thread, we'd wake up and
        // realize we received a large result.  In theory we could call
        // 'tre.response()' right now, enabling this Node without the UDP packet
        // hop-hop... optimize me Some Day.
      }
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      int off     = UDP.SZ_TASK; // Skip udp byte and port and task#
      byte flag   = buf[off++];  // Flag for udp/tcp
      if( flag == 0 || flag == 2 ) {
        off += 3;               // 3 byets of zero classloader
        byte rf     = buf[off++];
        int klen    = get2(buf,off); off+=2;
        return "task# "+tasknum+" "+flag+" key["+klen+"]="+(char)buf[off]+(char)buf[off+1];
      } else {
        return "task# "+tasknum+" "+flag+" TCP "+((flag==1)?"pack":"reply");
      }
    }
  }

  // Unpack the answer
  protected T unpack( DatagramPacket p ) {
    // Cleanup after thyself
    if( _did_put ) DKV.remove(_args);
    if( _dt.void_result() ) return _dt;
    // First SZ_TASK bytes have UDP type# and port# and task#.
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    // Read object off the wires
    if( buf[off++] == 2 ) {     // Result is coming via TCP or UDP?
      _dt.read(buf,off);        // UDP result
    } else {
      // Big object, switch to TCP style comms.  Should have already done a
      // DRemoteTask read from the TCP receiver thread... so no need to read here.
    }
    return _dt;
  }
}
