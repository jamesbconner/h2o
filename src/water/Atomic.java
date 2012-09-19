package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic extends RemoteTask {
  Key _key;                     // Transaction key
  TaskRemExec _tre;             // The remote-execution cookie

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

  public Atomic() {}

  // User's function to be run atomically.  The Key's Value is fetched from the
  // home STORE, and the bits are passed in.  The returned bits are atomically
  // installed as the new Value (the function is retried until it runs
  // atomically).  The original bits are supposed to be read-only.
  abstract public byte[] atomic( byte[] bits );
  // override this if you need to perform some action after the update succeeds (eg cleanup)
  public void onSuccess(){}
 
  // Block until it completes, if run remotely
  @Override public final void invoke( Key key ) {
    fork(key);
    if( _tre != null ) _tre.get();
  }

  // Fork off
  public final void fork( Key key ) {
    _key = key;
    if( key.home() ) {          // Key is home?
      compute();                // Also, run it blocking/now
    } else {                    // Else run it remotely
      _tre = new TaskRemExec(key.home_node(),this,key,UDP.udp.atomic);
    }
  }

  // The (remote) workhorse:
  @Override public final void compute( ) {
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
      assert bits1 == null || bits1 != bits2; // No returning the same array

      // Attempt atomic update
      Value val2 = new Value(_key,bits2);
      Value res = DKV.DputIfMatch(_key,val2,val1);
      if( res == val1 ) {
        onSuccess();
        return; // Success?
      }      
      // and retry
    }
  }
}
