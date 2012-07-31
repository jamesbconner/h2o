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

public class TaskPutKey extends DFutureTask<Object> {

  final Key _key; // The LOCAL Key; presumably a bare Key no Value, and NOT interned
  final Value _val; // Value to be pushed

  // Asking the remote for the Value matching this specific Key.  Return the
  // first len bytes of that key.  If len==Integer.MAX_VALUE the intent is to
  // cache the entire key locally.
  protected TaskPutKey( H2ONode target, Key key, Value val ) {
    super( target,UDP.udp.putkey );
    _key  = key;
    _val = val;

    // A Stronger MM is easier, in many ways than an weaker one.  In this case
    // I am ordering writes from the same Node to the same Key, so that the
    // last write from this Node wins.  The specific use-case is simply PUT
    // then REMOVE, where the PUT and REMOVE swap over the wires and the REMOVE
    // happens before any PUT, the PUT sticks and a Key is leaked.

    // The fix is to stall earlier outgoing writes to the same Key until they
    // have completed, i.e., I get the ACK back.  One pass through the pending
    // TASKs is sufficient, because this thread cannot issue any more PUTs
    // until the current one finishes.
    for( DFutureTask dt : TASKS.values() )
      if( dt != this && dt._target == target &&
          dt instanceof TaskPutKey && ((TaskPutKey)dt)._key.equals(key) )
        dt.get();               // Stall till done to force ordering with this TPK

    resend();                   // Initial send after final fields set
  }

  // Pack key+value into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = UDP.SZ_TASK;      // Skip udp byte and port and task#
    if( _val == null || !_val.is_goal_persist() ) { // This is a send of a deleted value
      off = _key.write(buf,off);
      buf[off++] = 0;           // Deleted sentinel
      return off;
    }
    int len = _val._max < 0 ? 0 : _val._max;
    if( off+_key.wire_len()+_val.wire_len(len) <= MultiCast.MTU ) { // Small Value!
      off = _key.write(buf,off);
      off = _val.write(buf,off,len);
      return off;
    } else {

      // Big Object goes via TCP!  Note that this is synchronous right now: we
      // block in the TCP write call until we're done.  Since TCP is reliable
      // we dont need a UDP packet for cmd/ack - but we're using the rest of
      // the TaskPutKey logic to order writes... e.g. lest a large value is
      // mid-write, when the REMOVE comes along and "passes" it in the wires.
      synchronized(_target) {       // Only open 1 TCP channel to that H2O at a time!
      Socket sock = null;
      try {
        sock = new Socket( _target._key._inet, _target._key.tcp_port() );
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        // Write out the initial operation & key
        dos.writeByte(UDP.udp.putkey.ordinal());
        dos.writeShort(H2O.UDP_PORT);
        dos.writeInt(_tasknum);
        _key.write(dos);
        // Start the (large) write
        _val.write(dos,len);
        dos.flush(); // Flush before trying to get the ack
        InputStream is = sock.getInputStream();
        int ack = is.read(); // Read 1 byte of ack
        if( ack != 99 ) throw new IOException("missing tcp ack "+ack);
        sock.close();
        return off;
      } catch( IOException e ) {
        try { if( sock != null ) sock.close(); }
        catch( IOException e2 ) { /*no msg for error on closing broken socket */}
        // Be silent for SocketException; we get this if the remote dies and we
        // basically expect them.
        if( !(e instanceof SocketException) ) // We get these if the remote dies mid-write
          System.err.println("tcp socket failed "+e);
        return off;
      }
      }
    }
  }

  // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
  // Node, not local.
  public static class RemoteHandler extends UDP {
    // Received a request to put a key
    void call(DatagramPacket p, H2ONode sender) {
      // Unpack the incoming arguments
      byte[] buf = p.getData();
      UDP.clr_port(buf); // Re-using UDP packet, so side-step the port reset assert
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      if( p.getLength() > off ) { // Empty TPKs are actual large Values sent via TCP
        Key key = Key.read(buf,off);
        off += key.wire_len();
        Value val = Value.read(buf,off,key);
        update(key,val,sender);
        off = UDP.SZ_TASK;              // Skip udp byte and port and task#
      }
      // Send it back
      reply(p,off,sender);
    }

    // TCP large K/V RECEIVE on the target from the remote.  Note that 'this'
    // is NOT the TaskPutKey object that is hoping to get the received object,
    // nor is the current thread the TPK thread blocking for the object.  The
    // current thread is the TCP reader thread.
    void tcp_read_call( DataInputStream dis, H2ONode h2o ) throws IOException {
      // Read all the parts
      int tnum = dis.readInt();
      Key key = Key.read(dis);
      // Big Read of Big Value
      Value val = Value.read(dis,key);
      update(key,val,h2o);
    }

    // Update the local target STORE given this pair from the remote.
    private void update( Key key, Value val, H2ONode sender ) {
      assert key.home() || val==null; // Only PUT to home for keys, or remote invalidation from home
      // We are about to update the local STORE for this key.
      // All known replicas will become invalid... except the sender.
      // Clear his replica-bits so we do not invalidate him.
      key. clr_mem_replica(sender);
      key.clr_disk_replica(sender);
      // Home-node side PUT (which may require invalidates)
      DKV.put(key,val);
      // Now we assume the sender is still valid in ram.
      if( val != null ) key.set_mem_replica(sender);
      // There's a weird race where another thread is writing also, and we set
      // blind-set the mem-replica field... preventing invalidates.  If we
      // see a change, force an invalidate to reload
      Value v2 = H2O.get(key);
      if( val != v2 && v2 != null && !v2.true_ifequals(val) )
        key.invalidate(sender);
    }


    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    public String print16( byte[] buf ) {
      int udp     = get_ctrl(buf);
      int port    = get_port(buf);
      int tasknum = get_task(buf);
      int off = UDP.SZ_TASK;    // Skip udp byte and port and task#
      byte rf     = buf[off++];            //  8
      int klen    = get2(buf,off); off+=2; // 10
      return "task# "+tasknum+" key["+klen+"]="+new String(buf,10,Math.min(klen,6));
    }
  }

  // Unpack the answer: there is none!  There is a bulkier version which
  // returns the old value.
  protected Value unpack( DatagramPacket p ) {
    return null;
  }
}
