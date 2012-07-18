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
  // This is a WEAK update: it is only strongly ordered with other updates to
  // the SAME key on the SAME node.
  static public void put( Key key, Value val ) {
    assert val.is_same_key(key);
    val.init_weak_vector_clock(VectorClock.NOW);
    Value res = put_and_replicate(key,val);
    if( res != null ) res.free_mem(); // Free the memory
  }
  static protected Value put_return_old( Key key, Value val ) {
    assert val.is_same_key(key);
    val.init_weak_vector_clock(VectorClock.NOW);
    // Caller must free the memory of the returned value
    return put_and_replicate(key,val);
  }

  // Remove this Key: really writes a new tombstone deleted Value, with the
  // current VectorClock so a late-arriving Put is correctly ignored.
  static public void remove( Key key ) {
    // We have a subtle race to avoid here: we want to prevent a racing update
    // to the Key changing the underlying Value's persistence layer from e.g.
    // Ice to HDFS - because the deleted-Value needs to be of the correct
    // persistence-flavor.  So we get a VC for the delete operation FIRST, then
    // get the old flavor Value.  During the PUT attempt, if the Value changes
    // it must be to a strictly newer VC time, so our attempted delete operation
    // will have been superceded by a more recent put.  If the Value has not
    // changed then our delete VC will be the most recent update.
    VectorClock vc = VectorClock.NOW.weak_vc();
    long weak = vc.weak_long();
    Value v = get(key);
    if( v == null ) return;
    Value res = put_and_replicate(key, v.makeDeleted(key,vc,weak));
    if( res != null ) res.free_mem(); // Free the memory
  }

  // Do a PUT, and on success trigger replication.
  static private Value put_and_replicate( Key key, Value val ) {
    // local update first, since this is a weak update
    Value res = H2O.put_if_later(key,val);
    if( res == val ) {
      throw new Error("untested, collapse identical writes");
      //return;
    }

    H2O cloud = H2O.CLOUD;      // Cloud used for replication
    DatagramPacket pack = UDPReceiverThread.get_pack();
    byte[] buf = pack.getData();
    for( int i=0; i<key.desired(); i++ ) {
      int idx = cloud.D(key,i);
      if( idx == -1 ) break; // Short count of replicas (cloud too small?)
      H2ONode target = cloud._memary[idx];
      if( target != H2O.SELF &&
          !key.is_disk_replica(target) ) // Unless we know replica has a copy...
        // Eagerly inform replica of an updated key
        UDPHazKeys.build_and_send( target, buf, key, val );
    }
    UDPReceiverThread.free_pack(pack);

    return res;
  }
  
  // User-Weak-Get a Key from the distributed cloud.
  static public Value get( Key key, int len ) {
    while( true ) {
      // Read the Cloud once per put-attempt, to keep a consistent snapshot.
      H2O cloud = H2O.CLOUD;
      Value val = H2O.get(key);
      // Hit in local cache?
      if( val != null ) {
        // No rolling backwards in vector time
        assert !VectorClock.NOW.happens_before(val);
        // See if we have enough data cached locally
        if( val.is_deleted() ) return null; // The special "deleted key" Value is not visible to clients
        if( len > val._max ) len = val._max;
        if( len == 0 ) return val;
        if( len <= val.get(len).length ) // We get something?
          return val;             // Done!
      }

      // While in theory we could read from any replica, we always need to
      // inform the home-node that his copy has been Shared... in case it
      // changes and he needs to issue an invalidate.  For now, always and only
      // fetch from the Home node.
      int idx = cloud.D(key,0); // Distribution function
      
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
   * not exist, creates it.
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
