package water;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import jsr166y.CountedCompleter;

/**
 * A remote execution request
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class TaskRemExec<T extends RemoteTask> extends DFutureTask<T> {
  private static final byte SERVER_UDP_SEND = 10;
  private static final byte SERVER_TCP_SEND = 11;
  private static final byte CLIENT_UDP_SEND = 12;
  private static final byte CLIENT_TCP_SEND = 13;
  private static final byte TCP_INCOMING_REXEC = 14;
  private static final byte TCP_OUTGOING_REXEC = 15;

  public final T _dt;  // Task to send & execute remotely
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
  }

  // Pack classloader/class & the instance data into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    Class<? extends RemoteTask> clazz = _dt.getClass();
    String sclazz = clazz.getName();  // The exact classname to execute

    // Then the instance data.
    int off = UDP.SZ_TASK             // Skip udp byte and port and task#
      + 1 // udp/tcp flag
      + 2 // string len
      + sclazz.length()
      + _args.wire_len()
      + _dt.wire_len();
    if( off <= MultiCast.MTU ) {
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      Stream stream = new Stream(buf, off);
      stream.set1(SERVER_UDP_SEND);
      stream.setLen2Str(sclazz);
      _args.write(stream);
      _dt.write(stream);
      off = stream._off;
    } else {                    // Big object, switch to TCP style comms.
      off = UDP.SZ_TASK;        // Skip udp byte and port and task#
      buf[off++] = SERVER_TCP_SEND;
      while( !tcp_send_pack(new Byte(TCP_INCOMING_REXEC),sclazz,_args,_dt) ) {
        // If TCP fails, assume it's an overloaded network, and try again
        // after a bit.
        _tcp_started = false;   // Allow a retry
        try { Thread.sleep(100); } catch( InterruptedException e ) { }
      }
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

      Stream s = new Stream(buf, UDP.SZ_TASK); // Skip udp byte and port and task#
      byte cmd = s.get1();
      if( cmd == SERVER_UDP_SEND) {
        String clazz = s.getLen2Str();
        Key args = Key.read(s);

        // Make a remote instance of this dude from the stream
        RemoteTask dt = RemoteTask.make(clazz);

        // Fill in the remote values
        dt.read(s);

        remexec(dt, args, p, h2o);
      } else {
        assert cmd == SERVER_TCP_SEND;
        // Else all the work is being done in the TCP thread
        // No "reply" here: the TCP thread will ship a reply.  Also, the packet
        // is not-yet-dead: it is the eventual reply packet, and it is recorded
        // in the H2ONode task list.
      }
    }

    // Do the remote execution in a F/J thread & send a reply packet
    static void remexec( RemoteTask dt, Key args, DatagramPacket p, H2ONode h2o ) {
      // Now compute on it!
      dt.invoke(args);

      byte[] buf = p.getData();
      byte b = buf[UDP.SZ_TASK];
      assert b==SERVER_UDP_SEND || b==SERVER_TCP_SEND : "remexec reply pack busted: "+b;

      // Send it back
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      if( dt.wire_len()+off+1 <= MultiCast.MTU ) {
        Stream s = new Stream(buf, UDP.SZ_TASK);
        s.set1(CLIENT_UDP_SEND); // Result coming via UDP
        dt.write(s);
        off = s._off;
      } else {
        buf[off++] = CLIENT_TCP_SEND;
        // Push the large result back *now* (no async pause) via TCP
        while( !tcp_send(h2o,UDP.udp.rexec,get_task(buf),TCP_OUTGOING_REXEC,dt) ) {
          // If TCP fails, assume it's an overloaded network, and try again
          // after a bit.
          try { Thread.sleep(100); } catch( InterruptedException e ) { }
        }
      }
      assert buf[UDP.SZ_TASK]==CLIENT_UDP_SEND || buf[UDP.SZ_TASK]==CLIENT_TCP_SEND;
      assert off > UDP.SZ_TASK;
      reply(p,off,h2o);
    }

    // TCP large DRemoteTask RECEIVE of results.  Note that 'this' is NOT the
    // TaskRemExec object that is hoping to get the received object, nor is the
    // current thread the TRE thread blocking for the object.  The current
    // thread is the TCP reader thread.
    void tcp_read_call( DataInputStream dis, final H2ONode h2o ) throws IOException {
      // Read all the parts
      int tnum = dis.readInt();
      int flag = dis.readByte(); // 1==setup, 2==response
      assert flag==TCP_INCOMING_REXEC || flag==TCP_OUTGOING_REXEC;

      if( flag==TCP_INCOMING_REXEC ) {
        // Read clazz string
        String clazz = TCPReceiverThread.readStr(dis);
        final Key args = Key.read(dis);

        // Make a remote instance of this dude
        final RemoteTask dt = RemoteTask.make(clazz);
        // Fill in the remote values
        dt.read(dis);

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
        buf[UDP.SZ_TASK] = SERVER_TCP_SEND; // Just the "send by TCP" flag
        p1.setLength(UDP.SZ_TASK+1);
        DatagramPacket p2 = h2o.putIfAbsent(p1); // UDP race to insert pack
        if( p2 != null ) UDPReceiverThread.free_pack(p1); // UDP raced ahead of us?
        final DatagramPacket p = p2==null ? p1 : p2;

        // Here I want to execute on this, but not block for completion in the
        // TCP reader thread.
        udp.pool().execute(new CountedCompleter() {
            public void compute() {
              remexec(dt, args, p, h2o);
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
    @SuppressWarnings("unused")
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      Stream s = new Stream(buf,UDP.SZ_TASK); // Skip udp byte and port and task#
      byte flag   = s.get1();                 // Flag for udp/tcp
      String clazz= "";                       // Rexec class
      if( flag == SERVER_UDP_SEND || flag == SERVER_TCP_SEND ) {
        int slen = s.get2();    // String clazz len
        slen = Math.min(slen,s._buf.length-s._off);
        clazz = new String(buf,s._off,slen);
      }
      String fs="";
      switch( flag ) {
      case SERVER_UDP_SEND: fs = "SERVER_UDP_SEND"; break;
      case SERVER_TCP_SEND: fs = "SERVER_TCP_SEND"; break;
      case CLIENT_UDP_SEND: fs = "CLIENT_UDP_SEND"; break;
      case CLIENT_TCP_SEND: fs = "CLIENT_TCP_SEND"; break;
      case TCP_INCOMING_REXEC: fs = "TCP_INCOMING_REXEC"; break;
      case TCP_OUTGOING_REXEC: fs = "TCP_OUTGOING_REXEC"; break;
      }
      return "task# "+tasknum+" "+fs+" "+clazz;
    }
  }

  // Unpack the answer
  protected T unpack( DatagramPacket p ) {
    // Cleanup after thyself
    if( _did_put ) DKV.remove(_args);
    // First SZ_TASK bytes have UDP type# and port# and task#.
    byte[] buf = p.getData();
    Stream s = new Stream(buf, UDP.SZ_TASK); // Skip udp byte and port and task#
    // Read object off the wires
    int cmd = s.get1();
    if( cmd == CLIENT_UDP_SEND ) {
      _dt.read(s);
    } else {
      assert cmd == CLIENT_TCP_SEND : "found cmd "+cmd+" while unpack a large "+(_dt.getClass());
      // Big object, switch to TCP style comms.  Should have already done a
      // DRemoteTask read from the TCP receiver thread... so no need to read here.
    }
    return _dt;
  }
}
