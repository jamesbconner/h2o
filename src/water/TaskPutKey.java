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
  final Value _old; // "Helper" return value for DKV.put; not used here

  // Asking the remote for the Value matching this specific Key.  Return the
  // first len bytes of that key.  If len==Integer.MAX_VALUE the intent is to
  // cache the entire key locally.
  protected TaskPutKey( H2ONode target, Key key, Value val, Value old ) {
    super( target,UDP.udp.putkey );
    _key  = key;
    _val = val;
    _old = old;

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
    if( _val == null ) {        // This is a send of a deleted value
      off = _key.write(buf,off);
      buf[off++] = 0;           // Deleted sentinel
      return off;
    }
    int len = _val._max < 0 ? 0 : _val._max;
    if( off+_key.wire_len()+_val.wire_len(len) <= MultiCast.MTU ) { // Small Value!
      off = _key.write(buf,off);
      Stream s = new Stream(buf, off);
      _val.write(s, len);
      off = s._off;
    } else {
      // Big Object goes via TCP!  Note that this is synchronous right now: we
      // block in the TCP write call until we're done.  Since TCP is reliable
      // we do not need a UDP packet for cmd/ack - but we're using the rest of
      // the TaskPutKey logic to order writes... e.g. lest a large value is
      // mid-write, when the REMOVE comes along and "passes" it in the wires.
      while( !tcp_send_pack(_key,_val,_val.get()) ) {
        // If TCP fails, assume it's an overloaded network, and try again
        // after a bit.
        _tcp_started = false;   // Allow a re-try
        try { Thread.sleep(100); } catch( InterruptedException e ) { }
      }
    }
    return off;
  }

  // Handle the remote-side incoming UDP packet.  This is called on the REMOTE
  // Node, not local.
  public static class RemoteHandler extends UDP {
    // Received a request to put a key
    void call(DatagramPacket p, H2ONode sender) {
      assert Thread.currentThread().getPriority() == Thread.MAX_PRIORITY-2;
      // Unpack the incoming arguments
      byte[] buf = p.getData();
      UDP.clr_port(buf); // Re-using UDP packet, so side-step the port reset assert
      if( p.getLength() > UDP.SZ_TASK ) { // Empty TPKs are actual large Values sent via TCP
        Stream s = new Stream(buf, UDP.SZ_TASK);
        Key key = Key.read(s);
        Value val = Value.read(s, key);
        update(key, val, sender);
      }
      // Send it back
      reply(p, UDP.SZ_TASK, sender);
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
      if( key.home() ) {        // Home-side put?
        DKV.put(key,val,sender);// Home-node side PUT (which may require invalidates)
      } else {                  // Remote put: only invalidate is allowed
        assert val == null;     // Invalidate only
        // Similar to DKV.put, except no further invalidates nor forwarding to home
        // Just wipe the local value out.
        Value old = H2O.get(key);
        while( H2O.putIfMatch(key,null,old) != old )
          old = H2O.get(key);   // Repeat until we invalidate something
      }
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
  protected Object unpack( DatagramPacket p ) {
    return _key;
  }

  // Can be a collection of TPKs... useful for blocking on a parallel
  // collection of independent pending writes.
  TaskPutKey _tpks[];
  protected TaskPutKey( Value old ) {
    super(null,UDP.udp.bad);
    _key = null;
    _val = null;
    _old = old;
    _tpks = new TaskPutKey[8];
  }
  // Blocking call for either the collection or upcall for the singleton case.
  public Object get() {
    if( _tpks == null ) return super.get();
    for( TaskPutKey tpk : _tpks ) if( tpk != null ) tpk.get();
    return null;
  }
}
