package water;
import java.io.*;
import java.util.Arrays;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic extends DRemoteTask {

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
  // atomically).  The original bits are supposed to be read-only.
  abstract public byte[] atomic( byte[] bits );
  // override this if you need to perform some action after the update succeeds (eg cleanup)
  public void onSuccess(){}
  // By default, nothing sent over with the function (except the target Key).
  protected int  wire_len() { return 0; }
  protected int  write( byte[] buf, int off ) { return off; }
  protected void write( DataOutputStream dos ) throws IOException { throw new Error("do not call"); }
  protected void read( byte[] buf, int off ) { }
  protected void read( DataInputStream dis ) throws IOException { throw new Error("do not call"); }
 
  // The (remote) workhorse:
  @Override public final void map( Key key ) {
    assert key.home();          // Key is at Home!
    while( true ) {
      Value val1 = DKV.get(key);
      byte[] bits1 = (val1 == null) ? null : val1.get();

      // Run users' function.  This is supposed to read-only from bits1 and
      // return new bits2 to atomically install.
      byte[] bits2 = atomic(bits1);
      assert bits1 == null || bits1 != bits2; // No returning the same array

      // Attempt atomic update
      Value val2 = new Value(key,bits2);
      Value res = DKV.DputIfMatch(key,val2,val1);
      if( res == val1 ) {
        onSuccess();
        return; // Success?
      }      
      // and retry
    }
  }

  @Override public final void reduce( DRemoteTask rt ) { }

  // By default, return no result from the Atomic operation
  protected boolean void_result() { return true; }
}
