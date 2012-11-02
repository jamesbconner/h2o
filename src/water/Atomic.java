package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic extends RemoteTask {
  public Key _key;              // Transaction key

  // Example use for atomic update of any Key:
  //  new Atomic(key_to_be_atomically_updated) {
  //    public void atomic( byte[] bits_for_key ) {
  //      long x = UDP.get8(bits_for_key,0);
  //      x++;
  //      byte[] bits2 = new byte[8];
  //      UDP.set8(bits2,0,x);
  //      return bits2;
  //    }
  //  }

  // User's function to be run atomically.  The Key's Value is fetched from the
  // home STORE, and the bits are passed in.  The returned bits are atomically
  // installed as the new Value (the function is retried until it runs
  // atomically).  The original bits are supposed to be read-only.  User can
  // abort the transaction (onSuccess is NOT executed) by returning the
  // original bits.
  abstract public byte[] atomic( byte[] bits );
  // override this if you need to perform some action after the update succeeds (eg cleanup)
  public void onSuccess(){}
 
  // Block until it completes, if run remotely
  @Override public final void invoke( Key key ) {
    TaskRemExec tre = fork(key);
    if( tre != null ) tre.get();
  }

  // Fork off
  public final TaskRemExec fork( Key key ) {
    _key = key;
    if( key.home() ) {          // Key is home?
      compute();                // Also, run it blocking/now
      return null;
    } else {                    // Else run it remotely
      return new TaskRemExec(key.home_node(),this,key,UDP.udp.atomic);
    }
  }

  // The (remote) workhorse:
  @Override public final void compute( ) {
    compute2();
    _key = null;                // No need for key no more
    tryComplete();              // Tell F/J this task is done
  }
  private final void compute2( ) {
    assert _key.home();         // Key is at Home!
    while( true ) {
      Value val1 = DKV.get(_key);
      byte[] bits1 = null;
      if( val1 != null ) {      // Got a mapping?
        bits1 = val1.get();     // Get the bits
        if( bits1 == null )     // Assume XTN failure & try again
          continue;             // No bits?  deleted value already?
      }

      // Run users' function.  This is supposed to read-only from bits1 and
      // return new bits2 to atomically install.
      byte[] bits2 = atomic(bits1);
      if( bits2 == bits1 ) return; // User aborts the transaction

      // Attempt atomic update
      Value val2 = new Value(_key,bits2);
      Object res = DKV.DputIfMatch(_key,val2,val1,H2O.SELF);
      if( res instanceof TaskPutKey ) {
        TaskPutKey tpk = (TaskPutKey)res;
        tpk.get();              // Block for the DputIfMatch to complete
        res = tpk._old;         // Pick up the results
        assert res == val1;     // We only get a block-result if we also succeeded
      }
      if( res == val1 ) {       // Success?
        onSuccess();            // Call user's post-XTN function
        return; 
      }
      // and retry
    }
  }
}
