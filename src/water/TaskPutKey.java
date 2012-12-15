package water;
import java.util.concurrent.Future;

/**
 * Push the given key to the remote node
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class TaskPutKey extends DTask {
  Key _key;
  Value _val;
  transient Value _xval;
  static void put( H2ONode h2o, Key key, Value val, Futures fs ) {
    // A Stronger MM is easier, in many ways than an weaker one.  In this case
    // I am ordering writes from the same Node to the same Key, so that the
    // last write from this Node wins.  The specific use-case is simply PUT
    // then REMOVE, where the PUT and REMOVE swap over the wires and the REMOVE
    // happens before any PUT, the PUT sticks and a Key is leaked.

    // The fix is to stall until earlier outgoing writes to the same Key
    // completed, i.e., I get the ACK back.  One pass through the pending TASKs
    // is sufficient, because this thread cannot issue any more PUTs until the
    // current one finishes.
    for( RPC rpc : RPC.TASKS.values() )
      if( rpc._target == h2o && rpc._dt instanceof TaskPutKey &&
          ((TaskPutKey)rpc._dt)._key == key )
        rpc.get();

    Future f = RPC.call(h2o,new TaskPutKey(key,val));
    if( fs != null ) fs.add(f);
  }

  static void invalidate( H2ONode h2o, Key key, Futures fs ) {
    Future f = RPC.call(h2o,new TaskPutKey(key,null));
    if( fs != null ) fs.add(f);
  }

  private TaskPutKey( Key key, Value val ) { _key = key; _xval = _val = val; }
  public TaskPutKey invoke( H2ONode sender ) {
    assert _key.home() || _val==null; // Only PUT to home for keys, or remote invalidation from home
    // Initialize Value for having a single known replica (the sender)
    if( _val != null ) _val.init_replica_home(sender,_key);

    // Spin, until we update something.
    Value old = H2O.get(_key);
    while( H2O.putIfMatch(_key,_val,old) != old )
      old = H2O.get(_key);       // Repeat until we update something.
    // Invalidate remote caches.  Block, so that all invalidates are done
    // before we return to the remote caller.
    if( _key.home() && old != null )
      old.lock_and_invalidate(sender,new Futures()).block_pending();
    // No return result
    _key = null;
    _val = null;
    return this;
  }
  @Override public void compute() { throw H2O.unimpl(); }

  // Received an ACK
  @Override public void onAck() {
    if( _xval != null ) _xval.put_completes();
  }
}
