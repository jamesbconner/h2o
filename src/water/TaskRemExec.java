package water;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;

import jsr166y.CountedCompleter;
import water.serialization.RemoteTaskSerializationManager;
import water.serialization.RemoteTaskSerializer;

/**
 * A remote execution request
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class TaskRemExec<T extends RemoteTask> extends DFutureTask<T> {
  private static final byte SERVER_UDP_SEND = 0;
  private static final byte SERVER_TCP_SEND = 1;
  private static final byte CLIENT_UDP_SEND = 2;
  private static final byte CLIENT_TCP_SEND = 3;
  private static final byte TCP_INCOMING_REXEC = 5;
  private static final byte TCP_OUTGOING_REXEC = 6;

  volatile T _dt;                         // Task to send & execute remotely
  final RemoteTaskSerializer<T> _serializer; // object to manage serialization
  final Key _args;
  final boolean _did_put;

  // With a Key+Value, do a Put on the Key & block for it - forcing the Value
  // to be available when remote execution starts.
  public TaskRemExec( H2ONode target, T dt, Key args, Value val ) {
    this( target, dt, args, true, UDP.udp.rexec );
    DKV.put(args,val);          // Publish the keyset for remote execution
    DKV.write_barrier();        // Block until all prior writes have completed
    resend();                   // Initial send after final fields set
  }

  // This version assumes a prior remote Value is already coherent
  public TaskRemExec( H2ONode target, T dt, Key args ) {
    this( target, dt, args, false, UDP.udp.rexec );
    resend();                   // Initial send after final fields set
  }

  public TaskRemExec(H2ONode target, T dt, Key key, UDP.udp type) {
    this(target, dt, key, false, type);
    resend();                   // Initial send after final fields set
  }

  @SuppressWarnings("unchecked")
  private TaskRemExec(H2ONode target, T dt, Key args, boolean did_put, UDP.udp type) {
    super( target, type );
    _dt = dt;
    _args = args;
    _did_put = did_put;
    _serializer = (RemoteTaskSerializer<T>) RemoteTaskSerializationManager.get(_dt.getClass());
  }

  // Pack classloader/class & the instance data into the outgoing UDP packet
  @SuppressWarnings("deprecation")
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    Class<? extends RemoteTask> clazz = _dt.getClass();
    String sclazz = clazz.getName();  // The exact classname to execute

    // Then the instance data.
    int off = UDP.SZ_TASK             // Skip udp byte and port and task#
      + 1 // udp/tcp flag
      + 3 // jarkey
      + 2 // string len
      + sclazz.length()
      + _args.wire_len()
      + _serializer.wire_len(_dt);
    if( off <= MultiCast.MTU ) {
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      buf[off++] = SERVER_UDP_SEND;
      // Class loader first.  3 bytes of null for system loader.
      buf[off++] = 0; // zero RF
      off += UDP.set2(buf,off,0); // 2 bytes of jarkey length

      // Class name now
      off += UDP.set2(buf,off,sclazz.length());  // String length
      sclazz.getBytes(0,sclazz.length(),buf,off); // Dump the string also
      off += sclazz.length();
      // Then the args key
      off = _args.write(buf,off);
      off = _serializer.write(_dt, buf,off);
    } else {                    // Big object, switch to TCP style comms.
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      buf[off++] = SERVER_TCP_SEND;
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
      byte cmd = buf[off++];
      if( cmd == SERVER_UDP_SEND) {
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
        RemoteTaskSerializer<RemoteTask> ser = RemoteTaskSerializationManager.get(clazz);

        // Fill in remote values
        RemoteTask dt = ser.read(buf, off);
        remexec(ser, dt, args, p, h2o);
      } else {
        assert cmd == SERVER_TCP_SEND;
        // Else all the work is being done in the TCP thread
        // No "reply" here: the TCP thread will ship a reply.  Also, the packet
        // is not-yet-dead: its the eventual reply packet, and it is recorded
        // in the H2ONode task list.
      }

    }

    // Do the remote execution in a F/J thread & send a reply packet
    static void remexec( RemoteTaskSerializer<RemoteTask> ser, RemoteTask dt,
                         Key args, DatagramPacket p, H2ONode h2o ) {
      // Now compute on it!
      dt.invoke(args);

      byte[] buf = p.getData();

      // Send it back
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      if( ser.wire_len(dt)+off+1 <= MultiCast.MTU ) {
        buf[off++] = CLIENT_UDP_SEND; // Result coming via UDP
        off = ser.write(dt, buf,off); // Result
      } else {
        buf[off++] = CLIENT_TCP_SEND;
        // Push the large result back *now* (no async pause) via TCP
        if( !tcp_send(h2o,UDP.udp.rexec,get_task(buf),TCP_OUTGOING_REXEC,dt) )
          return; // If the TCP failed... then so do we; no result; caller will retry
      }
      reply(p,off,h2o);
    }

    // TCP large DRemoteTask RECEIVE of results.  Note that 'this' is NOT the
    // TaskRemExec object that is hoping to get the received object, nor is the
    // current thread the TRE thread blocking for the object.  The current
    // thread is the TCP reader thread.
    @SuppressWarnings({ "unchecked", "rawtypes", "serial" })
    void tcp_read_call( DataInputStream dis, final H2ONode h2o ) throws IOException {
      // Read all the parts
      int tnum = dis.readInt();
      int flag = dis.readByte(); // 1==setup, 2==response
      assert flag==TCP_INCOMING_REXEC || flag==TCP_OUTGOING_REXEC;

      if( flag==TCP_INCOMING_REXEC ) {
        // Read clazz string
        int len = dis.readShort();
        byte[] bits = new byte[len];
        dis.readFully(bits);
        String clazz = new String(bits);
        final Key args = Key.read(dis);

        // Make a remote instance of this dude
        final RemoteTaskSerializer ser = RemoteTaskSerializationManager.get(clazz);
        final RemoteTask dt = ser.read(dis);

        // Need the UDP packet because the finish-up code expects one.  Act "as
        // if" called from the UDP packet code, by making the exact UDP packet
        // we will be recieving (eventually).  The presence of this packet is used
        // to stop dup-actions on dup-sends.
        DatagramPacket p1 = UDPReceiverThread.get_pack(); // Get a fresh empty packet
        final byte[] buf = p1.getData();
        UDP.udp udp = (dt instanceof Atomic) ? UDP.udp.atomic : UDP.udp.rexec;
        UDP.set_ctrl(buf,udp.ordinal());
        UDP.clr_port(buf);
        UDP.set_task(buf,tnum);
        buf[UDP.SZ_TASK] = 1;   // Just the "send by TCP" flag
        DatagramPacket p2 = h2o.putIfAbsent(p1);          // UDP race to insert pack
        if( p2 != null ) UDPReceiverThread.free_pack(p1); // UDP raced ahead of us?
        final DatagramPacket p = p2==null ? p1 : p2;

        // Here I want to execute on this, but not block for completion in the
        // TCP reader thread.
        udp.pool().execute(new CountedCompleter() {
            public void compute() {
              remexec(ser, dt, args, p, h2o);
              tryComplete();
            }
          });
        // All done for the TCP thread!  Work continues in the FJ thread...
      } else {
        assert flag == TCP_OUTGOING_REXEC;
        // Get the TGK we're waiting on
        TaskRemExec tre = (TaskRemExec)TASKS.get(tnum);
        // Race with canceling a large Value fetch: Task is already dead.  Do not
        // bother reading from the TCP socket, just bail out & close socket.
        if( tre == null ) return;

        // Big Read of Big Results
        tre._dt = tre._serializer.read(dis);
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
    @SuppressWarnings("unused")
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
    // First SZ_TASK bytes have UDP type# and port# and task#.
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    // Read object off the wires
    int cmd = buf[off++];
    if( cmd == CLIENT_UDP_SEND ) {
      _dt = _serializer.read(buf, off);
    } else {
      assert cmd == CLIENT_TCP_SEND : "found cmd "+cmd;
      // Big object, switch to TCP style comms.  Should have already done a
      // DRemoteTask read from the TCP receiver thread... so no need to read here.
    }
    return _dt;
  }
}
