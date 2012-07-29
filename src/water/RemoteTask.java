package water;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Constructor;
import jsr166y.*;

// Objects which are passed & remotely executed.  They have an efficient
// serialization (line-wire format).  On some remote target they are
// executed in the context of a Fork/Join worker thread.

// @author <a href="mailto:cliffc@0xdata.com"></a>
// @version 1.0
public abstract class RemoteTask extends RecursiveTask {
  // User overrides these methods to send his results back and forth.
  // Reads & writes user-guts to a line-wire format on a correctly typed object
  abstract protected int wire_len();
  abstract protected int  write( byte[] buf, int off );
  abstract protected void write( DataOutputStream dos );
  abstract protected void read( byte[] buf, int off );
  abstract protected void read( DataInputStream dis );

  // The Fork-Join hook.  We want users to override this for local computation
  abstract public Object compute();

  // Top-level remote execution hook.  Users should override this, but by
  // default it only calls the local compute.  Probably user really wants to do
  // something with this args Key.
  void rexec( Key args ) {
    compute();                  // Call 'compute' - probably not want the users' want!
  }

  // Make a RemoteTask
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
