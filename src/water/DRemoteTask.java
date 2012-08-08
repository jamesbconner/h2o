package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.Cloneable;
import jsr166y.*;

// *DISTRIBUTED* RemoteTask
// Execute a set of Keys on the home for each Key.
// Limited to doing a map/reduce style.

// @author <a href="mailto:cliffc@0xdata.com"></a>
// @version 1.0
public abstract class DRemoteTask extends RemoteTask implements Cloneable {

  // Some useful fields for *local* execution.  These fields are never passed
  // over the wire, and so do not need to be in the users' read/write methods.
  Key[] _keys;                  // Keys to work on
  int _lo, _hi;                 // Range of keys to work on

  // Run some useful function over this *local* key, and record the results in
  // the 'this' DRemoteTask.
  abstract public void map( Key key );
  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( RemoteTask drt );

  void ps(String msg) {
    Class clz = getClass();
    System.err.println(msg+clz+" keys["+_keys.length+"] _lo="+_lo+" _hi="+_hi);
  }

  // Make a copy of thyself
  DRemoteTask copy_self() {
    try { return (DRemoteTask)this.clone(); }
    catch( CloneNotSupportedException e ) { throw new Error(e); }
  }

  // Do all the keys in the list associated with this Node.  Roll up the
  // results into *this* DRemoteTask.
  public Object compute() {
    if( _keys.length==0 ) return null;
    if( _lo == -1 )
      return fork_by_cpus();

    if( _lo == _hi ) return null; // No data

    // Single key?
    if( _lo == _hi-1 ) {        // Single key?
      map(_keys[_lo]);          // Get it, run it locally
      return null;              // No return in any case
    }

    // Multi-key case: just divide-and-conquer down to 1 key
    final int mid = (_lo+_hi)>>1;   // Mid-point
    // Clone & fork
    DRemoteTask d1 = copy_self();
    d1._hi = mid;
    d1.fork();            // Runs in another thread/FJ instance
    // Also set self mid-points, then execute recursively
    this._lo = mid;
    compute();                  // Execute self on smaller half
    // Wait for the other 1/2 of results
    d1.join();
    // Reduce the two into self
    reduce(d1);                 // Users' reduction code
    return null;
  }

  // Split out the keys into disjointly-homed sets of keys.
  // Send the set that excludes self to some remote key home.
  // Recursivly work on the set that includes self.
  final public Object fork_by_cpus() {
    
    // Split out the keys into disjointly-homed sets of keys.
    // Find the split point.
    H2O cloud = H2O.CLOUD;
    int lo=cloud._memary.length, hi=-1;
    for( Key k : _keys ) {
      int i = k.home(cloud);
      if( i<lo ) lo=i;
      if( i>hi ) hi=i;        // lo <= home(keys) <= hi
    }

    // Are all keys local to self?
    // Then start local computations.
    int self_idx = cloud.nidx(H2O.SELF);
    if( lo == hi && self_idx == lo ) { // All local keys?
      _lo = 0;                         // Init the range of local keys
      _hi = _keys.length;
      return compute();         // Recursively compute - but locally now!
    }

    // We have multiply homed keys?  Split via fork/join
    int mid = (lo+(hi+1))>>1; // Mid-point
    // If no keys are local, arrange the split to just ship all keys to
    // somebody who IS local.
    if( self_idx < lo || self_idx > hi )
      mid = self_idx+1;
    
    // Classic fork/join, but on CPUs.
    // Split into 2 arrays of keys
    int size_remote_keys = 4;
    int num_local_keys = 0;
    Key remote_key = null;
    for( Key k : _keys ) {
      // the Key is on the same side of the split as 'self' then it is run
      // locally, else it will be run remotely.
      if( (self_idx < mid) ^ (k.home(cloud) < mid) ) {
        size_remote_keys += k.wire_len(); // Remote
        remote_key = k;                   // Some remote key
      } else {
        num_local_keys++;   // Local
      }
    }
    // Create storage for the 2 splits
    Key local_keys[] = new Key[num_local_keys];
    Key remote_args = Key.make(); // Random key
    Value rem_keys = new Value(remote_args,size_remote_keys);
    // Now fill in the two splits
    byte[] keybytes = rem_keys.mem();
    int off = 0;
    off += UDP.set4(keybytes,off,_keys.length - num_local_keys);
    int j = 0;
    for( Key k : _keys ) {
      // the Key is on the same side of the split as 'self' then it is run
      // locally, else it will be run remotely.
      if( (self_idx < mid) ^ (k.home(cloud) < mid) ) {
        off = k.write(keybytes,off);
      } else {
        local_keys[j++] = k;
      }
    }
    
    // Now fork remotely.
    H2ONode target = cloud._memary[remote_key.home(cloud)];
    TaskRemExec tre = new TaskRemExec(target,copy_self()/*result object*/,remote_args,rem_keys);
    
    // Setup for local recursion.
    _keys = local_keys;       // Keys, including local keys (if any)
    
    // Locally compute on the 1/2-sized set of keys into self.
    compute();
    
    RemoteTask drt = tre.get(); // Get the remote result
    
    reduce(drt);              // Combine results into self
    
    return null;              // Done!
  }

  // Top-level remote execution hook.  The Key is an ArrayLet or an array of
  // Keys; start F/J'ing on individual keys.  
  public void rexec( Key args ) {
    Value val = DKV.get(args);
    if( val == null ) {
      System.err.println("Missing args in rexec call: possibly the caller did not fence out a DKV.put(args) before calling rexec(,,args,).");
      throw new Error("Missing args");
    }
    Key[] keys = null;
    if( val.type() == Value.ARRAYLET ) {
      ValueArray ary = (ValueArray)val;
      keys = new Key[(int)ary.chunks()];
      for( int i=0; i<keys.length; i++ )
        keys[i] = ary.chunk_get(i);
    } else {
      // Parse all the keys out
      byte[] buf = val.get();
      int off = 0;
      int klen = UDP.get4(buf,off); off += 4;
      keys = new Key[klen];
      for( int i=0; i<klen; i++ ) {
        Key k = keys[i] = Key.read(buf,off);
        off += k.wire_len();
      }
    }

    rexec(keys);
  }

  // Handy constructor to fire off on an array of keys
  public void rexec( Key[] args ) {
    _keys = args;
    _lo = _hi = -1;   // Flag that we are still splitting remotely/globally
    H2O.FJP.invoke(this);  // Classic fork/join computation style.  Compute result into self.
  }
}
