package water;

/**
 * Atomic update of a Key
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public abstract class Atomic {
  final Key _key;

  // User's function to be run atomically.  The Key's Value is fetched from the
  // home STORE, and a clone of the bits are passed in.  The returned bits are
  // atomically installed as the new Value (the function is retried until it
  // runs atomically).
  abstract public void atomic( byte[] bits );

  public Atomic( Key key ) { _key = key; }

  public void run() {
    Class clz = getClass();
    System.out.println("Class= "+clz);
    throw new Error("unimplemented");
  }

}

