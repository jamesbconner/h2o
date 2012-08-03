package water;
import java.io.*;
import java.util.Arrays;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic extends RemoteTask {

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

  // For Now: pass ClassName in the wire packet, plus the target Key,

  TaskRemExec _tre; // Controls remote execution; used to allow blocking until the action completes


  // User's function to be run atomically.  The Key's Value is fetched from the
  // home STORE, and the bits are passed in.  The returned bits are atomically
  // installed as the new Value (the function is retried until it runs
  // atomically).  The original bits are supposed to be read-only.
  abstract public byte[] atomic( byte[] bits );

  // By default, nothing sent over with the function (except the target Key)
  protected int wire_len() { return 0; }
  protected int  write( byte[] buf, int off ) { return off; }
  protected void write( DataOutputStream dos ) { throw new Error("unimplemented"); }
  protected void read( byte[] buf, int off ) { }
  protected void read( DataInputStream dis ) { throw new Error("unimplemented"); }
  // Must define for the abstract class, but not needed:
  public Object compute() { throw new Error("Do Not Call This"); }

  // Start the remote atomic action on Key
  public void run( Key key ) {
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[key.home(cloud)]; // Key's home
    _tre = new TaskRemExec(target,this,key);
  }
  // Block until the remote action completes
  public final void complete() {
    _tre.get();
  }


  // The (remote) workhorse:
  protected final void rexec( Key key ) {
    assert key.home();          // Key is at Home!
    while( true ) {
      Value val1 = DKV.get(key);
      byte[] bits1 = val1.get();

      byte[] dummy = null;
      assert (dummy = bits1.clone()) != null; // Assign *inside* an array

      // Run users' function.  This is supposed to read-only from bits1 and
      // return new bits2 to atomically install.
      byte[] bits2 = atomic(bits1);

      assert Arrays.equals(dummy,bits1);
      assert bits1 != bits2;    // No returning the same array either.
      Value val2 = new Value(key,bits2);

      // Attempt atomic update
      Value res = DKV.DputIfMatch(key,val2,val1);

      if( res == val1 ) {       // Success?
        // Atomically updated!  Toss out old value
        System.out.println("updated");
        val1.free_mem();
        System.out.println("updated "+UDP.get8(val2.get(),0));
        return;
      }
      // Else it failed
      val2.free_mem();          // Toss out NEW value
      System.out.println("failed, retrying");
      // and retry
    }
  }
}

