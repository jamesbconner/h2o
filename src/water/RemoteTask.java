package water;
import jsr166y.CountedCompleter;

// Objects which are passed & remotely executed.  They have an efficient
// serialization (line-wire format).  On some remote target they are
// executed in the context of a Fork/Join worker thread.

// After much fun design work I figured on a fast solution.  Down The Road, we
// could improve this class so it mostly turns into a trivial packet
// send/receive of a token + args - with the receiever not doing much beyond
// doing an array lookup containing instances of the anonymous inner classes -
// and then dispatching the atomic-call on them.  This requires Cloud-wide
// agreement on what token maps to what instances.

// @author <a href="mailto:cliffc@0xdata.com"></a>
// @version 1.0
public abstract class RemoteTask extends CountedCompleter {

  // Top-level remote execution hook.  The Key is an ArrayLet or an array of
  // Keys; start F/J'ing on individual keys.  Blocks.
  abstract public void invoke( Key args );

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }
}
