package water;
import java.net.DatagramPacket;

/**
 * Distributed Key/Value Store
 *
 * This class handles the distribution pattern.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public abstract class DKV {

  // This put is a top-level user-update, and not a reflected or retried
  // update.  i.e., The User has initiated a change against the K/V store.
  // This is a WEAK update: it is not strongly ordered with other updates
  static public TaskPutKey put( Key key, Value val ) { return put(key,val,H2O.SELF); }
  static public TaskPutKey put( Key key, Value val, H2ONode sender ) {
    assert val==null || val.is_same_key(key);
    while( true ) {
      Value old = H2O.get(key);
      Object res = DputIfMatch(key,val,old,sender);
      if( res == old ) return null; // PUT worked without a TPK
      if( res instanceof TaskPutKey ) return (TaskPutKey)res;
    }
  }

  // Remove this Key: really writes a new tombstone deleted Value
  static public TaskPutKey remove( Key key ) { return put(key,null); }

  // Do a PUT, and on success trigger replication.  Some callers need the old
  // value, and some callers need the TPK so we can block later to ensure the
  // result is there.  Many callers don't need either value.  So rather than
  // making a special object to return the pair of values, I've settled for a
  // "callers pay" model with a more complex return setup.  The return value
  // is a TPK if one is needed, or the old Value if not.  If a TPK is returned
  // the old Value is stashed inside of it for the caller to consume.
  static protected Object DputIfMatch( Key key, Value val, Value old, H2ONode sender ) {
    if( val != null && sender != H2O.SELF )
      val.set_mem_replica(sender); // New Value will have at least 1 mem replica

    // local update first, since this is a weak update
    Value res = H2O.putIfMatch(key,val,old);
    if( res != old )            // Failed?
      return res;               // Return fail value

    // Check for trivial success: no need to invalidate remotes if the new
    // value equals the old.
    if( old != null && old == val ) return old; // Trivial success?
    if( old != null && val != null && val.true_ifequals(old) )
      return old;               // Less trivial success, but no network i/o

    // Before we start doing distributed writes... block until the cloud
    // stablizes.  After we start doing distrubuted writes, it is an error to
    // change cloud shape - the distributed writes will be in the wrong place.
    if( !Paxos._cloud_locked )
      Paxos.lock_cloud();

    // The 'D' part of DputIfMatch: do Distribution.
    // If PUT is on non-HOME, replicate/push to HOME
    // If PUT is on     HOME, invalidate remote caches
    if( key.home() ) {          // On     HOME?
      return old == null ? null : old.invalidate_remote_caches(sender);
    } else {                    // On non-HOME?
      H2O cloud = H2O.CLOUD;
      int home_idx = cloud.D(key,0);
      H2ONode target = cloud._memary[home_idx];
      // Start a write, but do not block for it.  Returns a cookie we can block
      // on, plus the old value buried in the cookie.
      return new TaskPutKey(target,key,val, old);
    }
  }

  // Stall until all existing writes have completed.
  // Used to order successive writes to unrelated keys.
  // Also blocks for invalidates
  static public void write_barrier() {
    for( DFutureTask dt : DFutureTask.TASKS.values() )
      if( dt instanceof TaskPutKey )
        dt.get();
  }

  // User-Weak-Get a Key from the distributed cloud.
  static public Value get( Key key, int len ) {
    while( true ) {
      // Read the Cloud once per put-attempt, to keep a consistent snapshot.
      H2O cloud = H2O.CLOUD;
      Value val = H2O.get(key);
      // Hit in local cache?
      if( val != null ) {
        // See if we have enough data cached locally
        if( len > val._max ) len = val._max;
        if( len == 0 ) return val;
        byte[] mem = val._mem;
        if( mem != null && len <= mem.length ) return val;
        if( val.is_persisted() ) return val; // Got it on local disk?  Then we must have it all
        // Got something, but not enough and not on local disk: need to read more
      }

      // While in theory we could read from any replica, we always need to
      // inform the home-node that his copy has been Shared... in case it
      // changes and he needs to issue an invalidate.  For now, always and only
      // fetch from the Home node.
      int home_idx = cloud.D(key,0); // Distribution function for home node
      H2ONode home = cloud._memary[home_idx];
      
      // If we missed in the cache AND we are the home node, then there is 
      // no V for this K (or we have a disk failure).
      if( home == H2O.SELF ) return null;

      // Pending write to same key from this node?  Take that write instead.
      // Moral equivalent of "peeking into the cpu store buffer".  Can happen,
      // e.g., because a prior 'put' of a null (i.e. a remove) is still mid-
      // send to the remote, so the local get has missed above, but a remote
      // get still might 'win' because the remote 'remove' is still in-progress.
      for( DFutureTask<?> dt : DFutureTask.TASKS.values() )
        if( dt._target == home && dt instanceof TaskPutKey && ((TaskPutKey)dt)._key.equals(key) )
          return ((TaskPutKey)dt)._val;

      TaskGetKey tgk = TaskGetKey.make( home, key, len );
      Value res = tgk.get();    // Block for it
      if( !tgk.isCancelled() )  // Task not canceled?
        return res;             // Then we got the desired result
      // Probably task canceled because the remote died; retry on new cloud
      throw new Error("untested: Cloud changed during a remote Get?");
    }
  }
  static public Value get( Key key ) { return get(key,Integer.MAX_VALUE); }
  
  
  static public void append(Key k, byte[] b) {
    append(k, b, 0, b.length);
  }

  /**
   * Appends the given bytes to the already existing value, if the value does
   * not exist, creates it.  Non-atomic update, so can drop racing writes.
   *
   * @param k
   * @param b
   * @param from
   * @param length
   */
  static void append(Key k, byte[] b, int from, int length) {
    Value old = get(k);
    Value value;
    if( old == null ) {
      value = new Value(k, length);
      System.arraycopy(b, from, value.mem(), 0, length);
    } else {
      assert (old.type() == Value.ICE); // only works on classic values
      value = new Value(k, old._max + length);
      System.arraycopy(old.get(), 0, value.mem(), 0, old._max);
      System.arraycopy(b, from, value.mem(), old._max, length);
    }
    put(k, value);
  }
   
}
