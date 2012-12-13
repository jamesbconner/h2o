package water;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import jsr166y.ForkJoinWorkerThread;

// *DISTRIBUTED* DTask
// Execute a set of Keys on the home for each Key.
// Limited to doing a map/reduce style.

// @author <a href="mailto:cliffc@0xdata.com"></a>
// @version 1.0
public abstract class DRemoteTask extends DTask<DRemoteTask> implements Cloneable {
  // Master key: either a Key-of-Keys (to distribute) or a single Key (same as
  // an array of 1 Key) or ValueVector (essentially a set of Keys for the
  // vector length).
  private Key _arg;
  boolean _delete_on_done;      // Key is temp; nuke after use

  // Some useful fields for *local* execution.  These fields are never passed
  // over the wire, and so do not need to be in the users' read/write methods.
  transient protected Key[] _keys; // Keys to work on

  // We can add more things to block on - in case we want a bunch of lazy tasks
  // produced by children to all end before this top-level task ends.
  transient private volatile Futures _fs; // More things to block on

  // Combine results from 'drt' into 'this' DRemoteTask
  abstract public void reduce( DRemoteTask drt );

  // Super-class init on the 1st remote instance of this object.  Caller may
  // choose to clone/fork new instances, but then is reponsible for setting up
  // those instances.
  public void init() { }

  // Make a copy of thyself, but set the (otherwise final) completer field.
  protected final DRemoteTask clone2() {
    try {
      DRemoteTask dt = (DRemoteTask)this.clone();
      dt.setCompleter(this); // Set completer, what used to be a final field
      dt.setPendingCount(0); // Volatile write for completer field; reset pending count also
      return dt;
    } catch( CloneNotSupportedException e ) { throw new Error(e); }
  }

  // Call with a master arg key, "as if" called from RPC.
  public DRemoteTask invoke( Key arg ) { _arg = arg; return fork( (H2ONode)null).get(); }
  public DFuture fork  ( Key arg ) { _arg = arg; return fork( (H2ONode)null); }

  // Invokes the task on all nodes
  public void invokeOnAllNodes() {
    H2O cloud = H2O.CLOUD;
    Key[] args = new Key[cloud.size()];
    String skey = "RunOnAll__"+UUID.randomUUID().toString();
    for( int i = 0; i < args.length; ++i )
      args[i] = Key.make(skey,(byte)0,Key.DFJ_INTERNAL_USER,cloud._memary[i]);
    invoke(args);
    for( Key arg : args ) DKV.remove(arg);
  }


  // Top-level remote execution hook (called from RPC).  Was passed the keys to
  // execute in _arg.  Fires off jobs to remote machines based on Keys.
  @Override public DRemoteTask invoke( H2ONode sender ) { return fork(sender).get(); }
  public DFuture fork( H2ONode sender ) {
    Key[] keys = flatten_keys(_arg);
    if( _delete_on_done ) DKV.remove(_arg);
    return fork(keys);
  }

  // Invoked with a set of keys
  public DRemoteTask invoke( Key[] keys ) {
    return fork(keys).get();
  }
  public DFuture fork( Key[] keys ) {
    _keys = keys;

    // Split out the keys into disjointly-homed sets of keys.
    // Find the split point.  First find the range of home-indices.
    H2O cloud = H2O.CLOUD;
    int lo=cloud._memary.length, hi=-1;
    for( Key k : _keys ) {
      int i = k.home(cloud);
      if( i<lo ) lo=i;
      if( i>hi ) hi=i;        // lo <= home(keys) <= hi
    }

    // Classic fork/join, but on CPUs.
    // Split into 3 arrays of keys: lo keys, hi keys and self keys
    final ArrayList<Key> locals = new ArrayList<Key>();
    final ArrayList<Key> lokeys = new ArrayList<Key>();
    final ArrayList<Key> hikeys = new ArrayList<Key>();
    int self_idx = cloud.nidx(H2O.SELF);
    int mid = (lo+hi)>>>1;    // Mid-point
    for( Key k : _keys ) {
      int idx = k.home(cloud);
      if( idx == self_idx ) locals.add(k);
      else if( idx < mid )  lokeys.add(k);
      else                  hikeys.add(k);
    }

    // Launch off 2 tasks for the other sets of keys, and get a place-holder
    // for results to block on.
    DFuture f = new DFuture(remote_compute(lokeys), remote_compute(hikeys));

    // Setup for local recursion: just use the local keys.
    _keys = locals.toArray(new Key[locals.size()]); // Keys, including local keys (if any)
    if( _keys.length != 0 ) {   // Shortcut for no local work
      init();                   // One-time top-level init
      H2O.FJP_NORM.submit(this);// Begin normal execution on a FJ thread
    }
    return f;             // Block for results from the log-tree splits
  }

  // Junk class only used to allow blocking all results, both local & remote
  public class DFuture {
    private final RPC<DRemoteTask> _lo, _hi;
    DFuture(RPC<DRemoteTask> lo, RPC<DRemoteTask> hi ) {
      _lo = lo;  _hi = hi;
    }
    // Block until completed, without having to catch exceptions
    public DRemoteTask get() {
      try {
        if( _keys.length != 0 )
          DRemoteTask.this.get(); // Block until the self-task is done
      } catch( InterruptedException ie ) {
        throw new RuntimeException(ie);
      } catch( ExecutionException ee ) {
        throw new RuntimeException(ee);
      }
      // Block for remote exec & reduce results into _drt
      if( _lo != null ) reduce(_lo.get());
      if( _hi != null ) reduce(_hi.get());
      if( _fs != null ) _fs.compute(); // Block on all other pending tasks, also
      return DRemoteTask.this;
    }
  };

  private final RPC<DRemoteTask> remote_compute( ArrayList<Key> keys ) {
    if( keys.size() == 0 ) return null;
    H2O cloud = H2O.CLOUD;
    Key arg = keys.get(0);
    H2ONode target = cloud._memary[arg.home(cloud)];
    DRemoteTask rpc = clone2();

    // Optimization: if sending just 1 key, and it is not a form which the
    // remote-side will expand, then send just the key, instead of a
    // key-of-keys containing 1 key.
    if( keys.size() ==1 && arg._kb[0] != Key.KEY_OF_KEYS && !arg.user_allowed() ) {
      // we can use arg directly
    } else {
      // pack our keys into a Key_of_Keys
      arg = Key.make(UUID.randomUUID().toString(), (byte)0, Key.KEY_OF_KEYS, target);
      Key[] xkeys = keys.toArray(new Key[keys.size()]);
      Value vkeys = new Value(arg, new AutoBuffer().putA(xkeys).buf());
      DKV.put(arg, vkeys);
      DKV.write_barrier();
      rpc._delete_on_done = true;
    }

    rpc._arg = arg;
    return RPC.call(target, rpc);
  }

  private final Key[] flatten_keys( Key args ) {
    Value val = DKV.get(args);
    if( args._kb[0] == Key.KEY_OF_KEYS ) { // Key-of-keys: break out into array of keys
      if( val == null ) {
        throw new Error("Missing args in fork call: " +
        		"possibly the caller did not fence out a DKV.put(args) " +
        		"before calling "+getClass()+".fork(,,args,) with key "+args);
      }
      return val.flatten();     // Parse all the keys out
    }

    // Arraylet: expand into the chunk keys
    if( val != null && val._isArray != 0 ) {
      ValueArray ary = ValueArray.value(val);
      Key[] keys = new Key[(int)ary._chunks];
      for( int i=0; i<keys.length; i++ )
        keys[i] = ary.getChunkKey(i);
      return keys;
    }
    // Handy wrap single key into a array-of-1 key
    return new Key[]{args};
  }

  public Futures getFutures() {
    if( _fs == null ) synchronized(this) { if( _fs == null ) _fs = new Futures(); }
    return _fs;
  }

  public void alsoBlockFor( Future f ) {
    if( f == null ) return;
    getFutures().add(f);
  }

  public void alsoBlockFor( Futures fs ) {
    if( fs == null ) return;
    getFutures().add(fs);
  }


  protected void reduceAlsoBlock( DRemoteTask drt ) {
    reduce(drt);
    alsoBlockFor(drt._fs);
  }
}
