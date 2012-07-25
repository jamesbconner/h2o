package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import jsr166y.RecursiveTask;

/**
 * Distributed RecursiveTask.  The granularity is over a *collection of keys*
 * Do remote-execution on a node where the key is homed.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
abstract public class DRecursiveTask extends RecursiveTask implements Cloneable {
  // User overrides this to reduce 2 of his answers to 1 of his answers
  public abstract DRecursiveTask reduce( DRecursiveTask d );
  // User overrides this to convert a Key to an answer, stored in 'this'
  public abstract void map( Key k );
  // User overrides these methods to send his results back and forth.
  // Reads & writes user-guts to a line-wire format on a correctly typed object
  abstract protected int wire_len();
  abstract protected void read( byte[] buf, int off );
  abstract protected void read( DataInputStream dis );
  abstract protected void write( byte[] buf, int off );
  abstract protected void write( DataOutputStream dos );


  // Some range of keys.  If _lo==_hi, then refers to a single Key.
  protected final Key[] _keys;
  public final Key _keykey;     // Key holding the Key array
  private int _lo, _hi;
  public final int idx() { return _lo; }
  // The distributed-code being executed (eventually becomes a process id)
  protected final Key _jarkey;

  // Only top-level user requests make new DRecursiveTasks.  Otherwise they are
  // cloned.  Top-level requests have the ValueCode.taskey() set.
  public DRecursiveTask( Key[] keys, int lo, int hi, Key jarkey ) { 
    _keys = keys;
    _lo = lo;
    _hi = hi;
    _jarkey = jarkey;
    assert jarkey != null;
    _keykey = null;
  }
  public DRecursiveTask( Key[] keys, int lo, int hi ) { this(keys,lo,hi, ValueCode._taskey.get()); }
  public DRecursiveTask( Key[] keys, Key jarkey ) { 
    _keys = keys;
    _lo = 0;
    _hi = keys.length;
    _jarkey = jarkey;
    assert jarkey != null;

    // Build & publish _keykey- the list of all keys.  We do this one-time at
    // the start of the task, because all the subtasks use this.
    _keykey = Key.make();      // Random key
    int size=4;                // Size of #keys
    for( Key k : keys )        // Compute total size of all keys
      size += k.wire_len();
    // New large-enuf Value
    Value vkeys = new Value(_keykey,size);
    byte[] mem = vkeys.mem();
    int off=0;                  // Now fill Value with keys
    off = UDP.set4_raw(mem,off,keys.length);
    for( Key k : keys )         // Write them all
      off = k.write(mem,off);
    DKV.put(_keykey,vkeys);
  }

  // "distributed" compute
  public final Object compute() {
    if( _lo == _hi ) return null; // No data
    // Multi-key case: just divide-and-conquer down to 1 key
    if( _lo < _hi-1 ) {
      final int mid = (_lo+_hi)>>1;   // Mid-point
      final int lo = _lo;
      final int hi = _hi;
      // Clone & fork
      DRecursiveTask d1;
      try { d1 = (DRecursiveTask)this.clone(); }
      catch( CloneNotSupportedException e ) { throw new Error(e); }
      d1._lo = lo;              // Set endpoints
      d1._hi = mid;
      d1.fork();

      // Also set self mid-points, then execute recursively
      _lo = mid;
      _hi = _hi;
      compute();                // Execute self on smaller half

      d1.join();

      DRecursiveTask dres = reduce(d1); // Users' reduction code
      assert dres == d1 || dres == this; // It is one or the other
      dres._lo = lo;                     // Reset endpoints on the winner
      dres._hi = hi;

      // Remove the temp KeyKey after reducing the very last job
      if( lo==0 && hi == _keys.length )
        UKV.remove(_keykey);
      return dres;
    }

    // Single-key case.  See *where* this should execute.
    // Execute local data locally, else ship code to data.
    ValueCode._taskey.set(_jarkey); // Set the process-id in case the users' code makes more
    Key k = _keys[_lo];
    if( k.home() ) {            // On the home for the key?
      map(k);                   // Then run it locally
    } else {
      Value localv = H2O.get(k); // Get any local value
      if( false && localv != null && localv.mem() != null && localv._max == localv.mem().length ) {
        System.out.println("doing key "+k+" on local cache");
        map(k);                 // Cached locally in ram: run it here
      } else {                  // Not home, not in ram; run it remotely
        remote_map();           // Run it remotely
      }
    }
    return null;
  }

  // Run this map remotely
  private final void remote_map( ) {
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[_keys[_lo].home(cloud)];
    // Remote execution
    TaskRemExec tre = new TaskRemExec( target, this );
    tre.get();                  // Block for result computation
  }
}
