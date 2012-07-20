package water;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.Socket;
import java.net.SocketException;

/**
 * Push the given key to the remote node
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskPutKey extends DFutureTask<Value> {

  final Key _key; // The LOCAL Key; presumably a bare Key no Value, and NOT interned
  final Value _val; // Value to be pushed

  // Asking the remote for the Value matching this specific Key.  Return the
  // first len bytes of that key.  If len==Integer.MAX_VALUE the intent is to
  // cache the entire key locally.
  protected TaskPutKey( H2ONode target, Key key, Value val ) {
    super( target,UDP.udp.putkey );
    _key  = key;
    _val = val;
    resend();                   // Initial send after final fields set
  }

  // Pack key+value into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = 1+2+4;          // Skip udp byte and port and task#
    int len = _val._max < 0 ? 0 : _val._max;
    if( off+_key.wire_len()+_val.wire_len(len) <= MultiCast.MTU ) { // Small Value!
      off = _key.write(buf,off);
      off = _val.write(buf,off,len);
      return off;
    } else {
      throw new Error("unimplemented");
    }
  }

  // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
  // Node, not local.
  public static class RemoteHandler extends UDP {
    // Received a request to put a key
    void call(DatagramPacket p, H2ONode h2o) {
      // Unpack the incoming arguments
      byte[] buf = p.getData();
      int off = 1+2+4;          // Skip udp byte and port and task#
      Key key = Key.read(buf,off);
      off += key.wire_len();
      Value val = Value.read(buf,off,key);
      System.out.println("TPK called with key "+key+" and now barfs");
      throw new Error("unimplemented");

      // Send it back
      //reply(p,off,h2o);
    }

    // TCP large K/V send from the remote to the target
    boolean tcp_recv( H2ONode h2o, Key key, Value val, int len, int tnum ) {
      throw new Error("unimplemented");
      //synchronized(h2o) {       // Only open 1 TCP channel to that H2O at a time!
      //Socket sock = null;
      //try {
      //  sock = new Socket( h2o._key._inet, h2o._key.tcp_port() );
      //  DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
      //  // Write out the initial operation & key
      //  dos.writeByte(UDP.udp.getkey.ordinal());
      //  dos.writeInt(tnum);
      //  key.write(dos);
      //  // Start the (large) write
      //  val.write(dos,len);
      //  dos.flush(); // Flush before trying to get the ack
      //  InputStream is = sock.getInputStream();
      //  int ack = is.read(); // Read 1 byte of ack
      //  if( ack != 99 ) throw new IOException("missing tcp ack "+ack);
      //  sock.close();
      //  return true;
      //} catch( IOException e ) {
      //  try { if( sock != null ) sock.close(); }
      //  catch( IOException e2 ) { /*no msg for error on closing broken socket */}
      //  // Be silent for SocketException; we get this if the remote dies and we
      //  // basically expect them.
      //  if( !(e instanceof SocketException) ) // We get these if the remote dies mid-write
      //    System.err.println("tcp socket failed "+e);
      //  return false;
      //}
      //}
    }

    // TCP large K/V RECIEVE on the target from the remote.  Note that 'this'
    // is NOT the TaskGetKey object that is hoping to get the received object,
    // nor is the current thread the TGK thread blocking for the object.  The
    // current thread is the TCP reader thread.
    void tcp_read_call( DataInputStream dis, H2ONode h2o ) throws IOException {
      throw new Error("unimplemented");
      //// Read all the parts
      //int tnum = dis.readInt();
      //Key key = Key.read(dis);
      //
      //// Get the TGK we're waiting on
      //TaskGetKey tgk = (TaskGetKey)TASKS.get(tnum);
      //// Race with canceling a large Value fetch: Task is already dead.  Do not
      //// bother reading from the TCP socket, just bail out & close socket.
      //if( tgk == null ) return;
      //assert tgk._key.equals(key);
      //// Big Read of Big Value
      //Value val = Value.read(dis,key);
      //// Single TCP reader thread, so _tcp_val is set single-threadedly
      //tgk._tcp_val = val;
      //// Here we have the Value, and we're on the correct Node but wrong
      //// Thread.  If we just return, the TCP reader thread will toss back a TCP
      //// ack to the remote, the remote will UDP ACK the TaskGetKey back, and
      //// back on the current Node but in the correct Thread, we'd wake up and
      //// realize we received a large Value.  In theory we could call
      //// 'tgk.response()' right now, enabling this Node without the UDP packet
      //// hop-hop... optimize me Some Day.
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      int off     = 1+2+4;
      byte rf     = buf[off++];            //  8
      int klen    = get2(buf,off); off+=2; // 10
      return "task# "+tasknum+" key["+klen+"]="+new String(buf,10,Math.min(klen,6));
    }
  }

  // Unpack the answer
  protected Value unpack( DatagramPacket p ) {
    throw new Error("unimplemented");
    //try {
    //  // First 7 bytes have UDP type# and port and task#.
    //  byte[] buf = p.getData();
    //  int off = 1+2+4;          // Skip udp byte and port and task#
    //  int len = UDP.get4(buf,off+2); // Get result length 2 bytes at the beginning of the value write are persistence and type
    //  if( len == -3 ) return null;   // Remote-miss
    //
    //  Value val = (len == /*Big Value cookie*/-2) ? _tcp_val : Value.read(buf,off,_key);
    //  // Need to officially put_if_later, in case of racing other updates
    //  Value old = H2O.STORE.get(_key);
    //  H2O.putIfMatch(_key,val,old);
    //  // If we succeeded, return Value.  If we failed, it means a racing write
    //  // superceded our write... but we can still return our write.  It is "as
    //  // if" we get in just prior to the racing other write.
    //  return val;
    //} finally {
    //  // Cleanup the dup-TaskGetKey-removal.  The next TGK to the same target &
    //  // key will start a fresh K/V fetch.
    //  _target.TGKS.remove(_key);
    //}
  }
}
