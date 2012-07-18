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
 * A remote request for a specifc key.  It is not distribution-aware, and just
 * returns the remote value if its' available.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskGetKey extends DFutureTask<Value> {

  final Key _key; // The LOCAL Key; presumably a bare Key no Value, and NOT interned
  final int _len; // Desired max length
  Value _tcp_val; // Only used for TCP receiving of large Values

  public static TaskGetKey make( H2ONode target, Key key, int len ) {
    return target.make_tgk(key,len);
  }

  // Asking the remote for the Value matching this specific Key.  Return the
  // first len bytes of that key.  If len==Integer.MAX_VALUE the intent is to
  // cache the entire key locally.
  protected TaskGetKey( H2ONode target, Key key, int len ) {
    super( target,UDP.udp.getkey );
    _key  = key;
    _len = len;
    resend();                   // Initial send after final fields set
  }

  // Pack key+len into the outgoing UDP packet
  protected int pack( DatagramPacket p ) {
    byte[] buf = p.getData();
    int off = 5;
    off += UDP.set4(buf,off,_len);
    off = _key.write(buf,off);
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
      int len = get4(buf,5);
      off += 4;
      // Get a local Key
      Key key = Key.read(buf,off);
      Value val = H2O.get(key); // Get the local Value, if any

      // Smack the result into a UDP packet
      // Missed Key sends back a length==-3
      off = 5;                  // Skip udp byte and task#
      if( val == null ) {       // Missed!  No Key
        buf[off++] = 0;         // Value-type == 0
        buf[off++] = 0;         // persistence
        off += set4(buf,off,-3); // Value-len == -3
      } else {
        if( len > val._max ) len = val._max; // Limit return bytes to _max
        if (val._max <0) len = 0; // this is important for sentinels
        if( off+val.wire_len(len,h2o) <= MultiCast.MTU ) { // Small Value!
          off = val.write(buf,off,len,h2o); // Just jam into reply packet
        } else {                        // Else large Value.  Push it over.
          // Push the large result back *now* (no async pause) via TCP
          if( !tcp_send(h2o,key,val,len,get4(buf,1)) )
            return; // If the TCP failed... then so do we; no result; caller will retry
          off = val.write(buf,off,-2/*tha Big Value cookie*/,h2o); // Just jam into reply packet
        }
      }

      // Send it back
      reply(p,off,h2o);
    }

    // TCP large K/V send from the remote to the target
    boolean tcp_send( H2ONode h2o, Key key, Value val, int len, int tnum ) {
      synchronized(h2o) {       // Only open 1 TCP channel to that H2O at a time!
      Socket sock = null;
      try {
        sock = new Socket( h2o._key._inet, h2o._key.tcp_port() );
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
        // Write out the initial operation & key
        dos.writeByte(UDP.udp.getkey.ordinal());
        dos.writeInt(tnum);
        key.write(dos);
        // Start the (large) write
        val.write(dos,len,h2o);
        dos.flush(); // Flush before trying to get the ack
        InputStream is = sock.getInputStream();
        int ack = is.read(); // Read 1 byte of ack
        if( ack != 99 ) throw new IOException("missing tcp ack "+ack);
        sock.close();
        return true;
      } catch( IOException e ) {
        try { if( sock != null ) sock.close(); }
        catch( IOException e2 ) { /*no msg for error on closing broken socket */}
        // Be silent for SocketException; we get this if the remote dies and we
        // basically expect them.
        if( !(e instanceof SocketException) ) // We get these if the remote dies mid-write
          System.err.println("tcp socket failed "+e);
        return false;
      }
      }
    }

    // TCP large K/V RECIEVE on the target from the remote.  Note that 'this'
    // is NOT the TaskGetKey object that is hoping to get the received object,
    // nor is the current thread the TGK thread blocking for the object.  The
    // current thread is the TCP reader thread.
    void tcp_read_call( DataInputStream dis, H2ONode h2o ) throws IOException {
      // Read all the parts
      int tnum = dis.readInt();
      Key key = Key.read(dis);

      // Get the TGK we're waiting on
      TaskGetKey tgk = (TaskGetKey)TASKS.get(tnum);
      // Race with canceling a large Value fetch: Task is already dead.  Do not
      // bother reading from the TCP socket, just bail out & close socket.
      if( tgk == null ) return;
      assert tgk._key.equals(key);
      // Big Read of Big Value
      Value val = Value.read(dis,key,h2o);
      // Single TCP reader thread, so _tcp_val is set single-threadedly
      tgk._tcp_val = val;
      // Here we have the Value, and we're on the correct Node but wrong
      // Thread.  If we just return, the TCP reader thread will toss back a TCP
      // ack to the remote, the remote will UDP ACK the TaskGetKey back, and
      // back on the current Node but in the correct Thread, we'd wake up and
      // realize we received a large Value.  In theory we could call
      // 'tgk.response()' right now, enabling this Node without the UDP packet
      // hop-hop... optimize me Some Day.
    }

    // Pretty-print bytes 1-15; byte 0 is the udp_type enum
    private static final byte [] keyname = new byte[4];
    public String print16( long lo, long hi ) {
      int udp     = (int)(lo&0xFF)        ; lo>>>= 8;
      int tasknum = (int)lo               ; lo>>>=32;
      int vlen = (int)(lo|((hi&0xFF)<<24)); hi>>>= 8;
      byte rf     = (byte)(hi&0xFF)       ; hi>>>= 8;
      short klen  = (short)(hi&0xFFFF)    ; hi>>>=16;
      set2(keyname,0,(int)hi);
      return "task# "+tasknum+" len="+((vlen==Integer.MAX_VALUE)?"all":String.valueOf(vlen))+" key["+klen+"]="+new String(keyname);
    }
  }

  // Unpack the answer
  protected Value unpack( DatagramPacket p ) {
    try {
      // First 5 bytes have UDP type# and task#.
      byte[] buf = p.getData();
      int len = UDP.get4(buf,5+2); // Get result length 2 bytes at the beginning of the value write are persistence and type
      if( len == -3 ) return null;   // Remote-miss

      Value val = (len == /*Big Value cookie*/-2) ? _tcp_val : Value.read(buf,5,_key,_target);
      // make sure that val is not marked as persisted or in progress - keep DO_NOT_PERSIST and CACHE values
      if ((val.persistenceState()==Value.IN_PROGRESS) || (val.persistenceState()==Value.PERSISTED))
        val.setPersistenceState(Value.NOT_STARTED);
      Value old = H2O.get(_key);
      // If we have this exact *Value* (not Key) already and are just extending
      // it... then just extend in place.
      if( old != null && old.same_clock(val) )
        return old.extend(val);
      // Not same value: need to officially put_if_later, in case of racing other updates
      Value res = H2O.put_if_later(_key,val);
      if( res != old ) throw new Error("unimplemented, fling out invalidates");
      return val;
    } finally {
      // Cleanup the dup-TaskGetKey-removal.  The next TGK to the same target &
      // key will start a fresh K/V fetch.
      _target.TGKS.remove(_key);
    }
  }

  // HashCode & equals, to clear out dup GetKeys - caused by repeated HazKeys
  public int hashCode() { return _key._hash ^ _target.hashCode() ^ _len; }
  public boolean equals( Object o ) {
    if( !(o instanceof TaskGetKey) ) return false;
    TaskGetKey tgk = (TaskGetKey)o;
    return _key.equals(tgk._key) && _target==tgk._target && _len == tgk._len;
  }
}
