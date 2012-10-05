package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
  @Override
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // Make a RemoteTask
  static final RemoteTask make( String clazz) {
    // Make a local instance and call map on it
    Exception e=null;
    try {
      return (RemoteTask)Class.forName(clazz).newInstance();
    }
    catch( ClassNotFoundException e0 ) { e=e0; }
    catch( IllegalAccessException e0 ) { e=e0; }
    catch( InstantiationException e0 ) { e=e0; }
    catch(   NullPointerException e0 ) { e=e0; }
    catch( UnsupportedClassVersionError e0 ) { e=new Exception(e0); }
    e.printStackTrace();
    System.err.println("puking "+e);
    return null;
  }

  // The abstract methods to be filled in by subclasses.  These are automatically
  // filled in by any subclass of RemoteTask during class-load-time, unless one
  // is already defined.  These methods are NOT DECLARED ABSTRACT, because javac
  // thinks they will be called by subclasses relying on the auto-gen.
  private Error barf() {
    return new Error(getClass().toString()+" should be automatically overridden in the subclass by the auto-serialization code");
  }
  public int wire_len()                                     { throw barf(); }
  public void write(DataOutputStream os) throws IOException { throw barf(); }
  public void read ( DataInputStream is) throws IOException { throw barf(); }
  public void write(          Stream  s)                    { throw barf(); }
  public void read (          Stream  s)                    { throw barf(); }
}
