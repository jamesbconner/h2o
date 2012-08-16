package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import jsr166y.*;

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
  // User overrides these methods to send his results back and forth.
  // Reads & writes user-guts to a line-wire format on a correctly typed object
  abstract protected int wire_len();
  abstract protected int  write( byte[] buf, int off );
  abstract protected void write( DataOutputStream dos ) throws IOException;
  abstract protected void read( byte[] buf, int off );
  abstract protected void read( DataInputStream dis ) throws IOException;

  // Top-level remote execution hook.  The Key is an ArrayLet or an array of
  // Keys; start F/J'ing on individual keys.  Blocks.
  abstract public void invoke( Key args );

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // By default, return the full user result.  But some jobs are "fire and
  // forget" and we do not want any result - this is a shortcut so the user
  // does not have to make the wire_len read/write fields be lopsided (i.e.
  // send-only, since by default they are send/recieve).
  protected boolean void_result() { return false; }

  // Make a RemoteTask from a wire-line format.  NO parent_task is possible.
  static final RemoteTask make( Key classloader, String clazz) {
    // Make a local instance and call map on it
    Exception e=null;
    try {
      // If this is a null classloader Key, then this is an internal function -
      // no need to pass or load an external jar file.
      Class klz;
      if( classloader == null ) {
        klz = Class.forName(clazz);
      } else {
        throw new Error("unimplemented");
        //Value v = DKV.get(classloader);
        //ValueCode val = (ValueCode)v;
        //ClassLoader h2o_ldr = val.getLoader();
        //klz = h2o_ldr.loadClass(clazz);
      }
      return (RemoteTask)klz.newInstance();
      //System.out.println("remote task class="+klz);
      //Constructor<RemoteTask> cdt = klz.getConstructor();
      //RemoteTask dt = cdt.newInstance();
      //return dt;
    } 
    catch( ClassNotFoundException    e0 ) { e=e0; }
    catch( IllegalAccessException    e0 ) { e=e0; }
    catch( InstantiationException    e0 ) { e=e0; }
    catch( NullPointerException      e0 ) { e=e0; }
    catch( UnsupportedClassVersionError e0 ) { e=new Exception(e0); }
    e.printStackTrace();
    System.err.println("puking "+e);
    return null;
  }
    
}
