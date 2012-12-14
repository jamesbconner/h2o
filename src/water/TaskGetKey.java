package water;
import water.nbhm.NonBlockingHashMap;

/**
 * Get the given key from the remote node
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskGetKey extends DTask {
  Key _key;                  // Set by client/sender JVM, cleared by server JVM
  Value _val;                // Set by server JVM, read by client JVM
  transient Key _xkey;       // Set by client, read by client
  transient H2ONode _h2o;    // Set by server JVM, read by server JVM on ACKACK

  // Unify multiple Key/Value fetches for the same Key from the same Node at
  // the "same time".  Large key fetches are slow, and we'll get multiple
  // requests close in time.  Batch them up.
  public static final NonBlockingHashMap<Key,RPC> TGKS = new NonBlockingHashMap();

  // Get a value from a named remote node
  public static Value get( H2ONode target, Key key ) {
    RPC<TaskGetKey> rpc;
    while( true ) {       // Repeat until we get a unique TGK installed per key
      // Do we have an old TaskGetKey in-progress?
      rpc = TGKS.get(key);
      if( rpc != null ) break;
      // Make a new TGK.
      rpc = new RPC(target,new TaskGetKey(key));
      if( TGKS.putIfMatchUnlocked(key,rpc,null) == null ) {
        rpc.call();             // Start the op
        break;
      }
      // Oops, colliding parallel RPC installs... try again
      rpc.cancel(true);         // Cancel the duplicate fetch
    }
    Value val = rpc.get()._val; // Block for, then fetch out the result
    TGKS.putIfMatchUnlocked(key,null,rpc); // Clear from cache
    return val;
  }

  private TaskGetKey( Key key ) { _key = _xkey = key; }

  // Top-level non-recursive invoke
  @Override public TaskGetKey invoke( H2ONode sender ) {
    _h2o = sender;
    Key k = _key;
    _key = null;          // Not part of the return result
    assert k.home();      // Gets are always from home (less we do replication)
    // Shipping a result?  Track replicas so we can invalidate.  There's a
    // narrow race on a moving K/V mapping tracking this Value just as it gets
    // deleted - in which case, simply retry for another Value.
    do  _val = H2O.get(k);      // The return result
    while( _val != null && !_val.set_replica(sender) );
    return this;
  }
  @Override public void compute() { throw H2O.unimpl(); }

  // Received an ACK; executes on the node asking&receiving the Value
  @Override public void onAck() {
    if( _val != null ) {        // Set transient fields after deserializing
      assert !_xkey.home() && _val._key == null;
      _val._key = _xkey;
    }
    // Now update the local store, caching the result.
    // Rules on when this can fail:
    // - Multiple active GETs for the same Key returning the same Value can be
    //   in flight at once.  However, they all return semantically the same
    //   Value ('equals' not '==').  Keeping either Value is correct.
    // - The Value is not allowed to change until no GETs are in-flight at the
    //   Home node.  However I cannot atomically call H2O.get-and-miss AND also
    //   start a TGK.  So this Value may be muchly delayed from the original
    //   local H2O.get-and-miss (with many updates inbetween and many TGK's
    //   refreshing the Value).  However, _val must be the latest result from
    //   Home, so keeping the new Value is correct.
    // - We can have a racing PUT from a local thread, racing against our
    //   returned Value (now stale) from Home.  This PUT Value will be flagged
    //   as in-progress, and must be very recent so the PUT Value is correct.
    Value old = H2O.get(_xkey);
    while( true ) {
      if( old != null && old.remote_put_in_flight() ) {
        _val = old;             // Old value is an existing PUT-in-flight
        break;                  // Take it as a more recent PUT
      }
      Value res = H2O.putIfMatch(_xkey,_val,old);
      if( res == old ) break;   // Success!
      old = res;                // Failed?  Changing values?
    }
  }

  // Received an ACKACK; executes on the node sending the Value
  @Override public void onAckAck() {
    if( _val != null ) _val.lower_active_gets(_h2o);
  }
}
