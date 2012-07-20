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
  static public void put( Key key, Value val ) {
    while( true ) {
      assert val.is_same_key(key);
      Value old = get(key);
      Value res = putIfMatch_and_replicate(key,val,old);
      if( res == old ) {
        if( old != null ) old.free_mem(); // Free the memory
        return;
      }
    }
  }
  static protected Value put_return_old( Key key, Value val ) {
    while( true ) {
      assert val.is_same_key(key);
      Value old = get(key);
      Value res = putIfMatch_and_replicate(key,val,old);
      // Caller must free the memory of the returned value
      if( res == old ) return old;
    }
  }

  // Remove this Key: really writes a new tombstone deleted Value
  static public void remove( Key key ) {
    while( true ) {
      Value old = get(key);
      if( old == null ) return; // Trivial success
      Value res = putIfMatch_and_replicate(key,old.makeDeleted(key),old);
      if( res == old ) {        // Worked?
        old.free_mem();         // Free the memory
        return;
      }
    }
  }

  // Do a PUT, and on success trigger replication.
  static private Value putIfMatch_and_replicate( Key key, Value val, Value old ) {
    // local update first, since this is a weak update
    Value res = H2O.putIfMatch(key,val,old);
    if( res != old )            // Failed?
      return res;               // Return fail value

    H2O cloud = H2O.CLOUD;      // Cloud used for replication
    for( int i=0; i<key.desired(); i++ ) {
      int idx = cloud.D(key,i);
      if( idx == -1 ) break; // Short count of replicas (cloud too small?)
      H2ONode target = cloud._memary[idx];
      if( target != H2O.SELF &&
          !key.is_disk_replica(target) ) // Unless we know replica has a copy...
        // Eagerly inform replica of an updated key, but do not block for it
        new TaskPutKey(target,key,val);
    }

    return old;                 // Return success value
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
        if( val.is_deleted() ) return null; // The special "deleted key" Value is not visible to clients
        if( len > val._max ) len = val._max;
        if( len == 0 ) return val;
        if( len <= val.get(len).length ) // We get something?
          return val;             // Done!
        // Got something, but not enough: need to read more
      }

      // While in theory we could read from any replica, we always need to
      // inform the home-node that his copy has been Shared... in case it
      // changes and he needs to issue an invalidate.  For now, always and only
      // fetch from the Home node.
      int idx = cloud.D(key,0); // Distribution function for home node
      
      // If we missed in the cache AND we are the home node, then there is 
      // no V for this K.
      if( cloud._memary[idx] == H2O.SELF ) return null;

      TaskGetKey tgk = TaskGetKey.make( cloud._memary[idx], key, len );
      Value res = tgk.get();    // Block for it
      if( !tgk.isCancelled() )  // Task not canceled?
        return res;             // Then we got the desired result
      // Probably task canceled because the remote died; retry on new cloud
      throw new Error("untested: Cloud changed during a remote Get");
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
