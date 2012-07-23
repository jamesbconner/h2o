package water;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import jsr166y.ForkJoinPool;

/**
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */
public class ValueCode extends Value {

  private JarLoader _loader;    // the jarloader for this value
  private Class _usr_kls;       // Class, if loaded
  private Method _main;         // The main method

  // Returns the singleton loader
  public JarLoader getLoader() {
    return _loader==null ? _loader = new JarLoader( get() ) : _loader;
  }

/*  @Override public Value create(int max, int len, VectorClock vc, long vcl, Key key, int state) {
    return new ValueCode(max,len,vc,vcl,key,state);
  } */
  
  // A "code" object: a verified class file, as raw bytes
  ValueCode( int max, int len, Key key, int mode ) {
    super( max, len, key, mode );
  }

  @Override public byte type() { return CODE; }

  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[code]");
    return true;
  }

  // Constructs a system ValueCode, and associates it with a returned Key.  The
  // ValueCode is NOT returned.  The Code is created by reading the given Value
  // klass, which is expected to be the bytes from a valid .jar file, and
  // verifying them and making them ready for distributed execution.

  // On success, a key is returned and the key is mapped to the ValueCode.
  // On failure, an error String is returned.
  public static Object compile( Key srcjar ) {
    Value klass = DKV.get(srcjar); // Fetch presumed bytecodes
    byte[] jarbits = klass.get();  // jar-file format
    String klazzname = srcjar.toString();
    try {
      // Make a decent attempt to validate the code.  Load & resolve it.
      CompilingClassLoader h2o_ldr = new CompilingClassLoader( jarbits );
      Class usr_kls = h2o_ldr.loadClass(klazzname);
      Method main = usr_kls.getMethod("main",String[].class);
      // Assign a unique key-name to the compiled jar file, so we can have many
      // different jars with the same name.
      String sha = signSHA(jarbits);
      String dclass = klazzname+".dclass" /*+ (sha==null?"":"."+sha)*/;
      Key obj = Key.make(dclass);
      // We like the class!  Furthermore we have made it "distribution ready"!
      // Wrapping it a ValueCode tells the rest of the system that it can load
      // this class fearlessly.
      ValueCode val = new ValueCode(jarbits.length,jarbits.length,obj,PersistIce.INIT);
      byte [] mem = val.mem();
      for(int i = 0; i < mem.length; ++i)
        mem[i] = jarbits[i];
      val._usr_kls = usr_kls;
      val._main = main;
      DKV.put(obj,val);
      return obj;
    } catch( ClassNotFoundException e ) {
      return e.toString(); // Probably fails because the keyname does not match the classname
    } catch( NoClassDefFoundError e ) {
      return e.toString(); // Probably fails because the keyname does not match the classname
    } catch( ClassFormatError e ) {
      return e.toString(); // Probably fails because the key was not a jar file
    } catch( NoSuchMethodException e ) {
      return e.toString(); // Probably fails because missing main method
    } catch( NoSuchMethodError e ) {
      return e.toString(); // Probably fails because missing main method
    }
  }
  
  // get unique signature for jar file, or return null
  private static String signSHA(byte[] a) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] sh = new byte[40];
      md.update(a, 0, a.length);
      sh = md.digest();
      StringBuffer buf = new StringBuffer();
      for( int i = 0; i < sh.length; i++ ) {
        int halfbyte = (sh[i] >>> 4) & 0x0F;
        int two_halfs = 0;
        do{
          if( (0 <= halfbyte) && (halfbyte <= 9) )
               buf.append((char) ('0' +  halfbyte      ));
          else buf.append((char) ('a' + (halfbyte - 10)));
          halfbyte = sh[i] & 0x0F;
        }while( two_halfs++ < 1 );
      }
      return buf.toString();
    } catch( NoSuchAlgorithmException e ) {
    }
    return null;
  }


  private Object get_main( ) { // Load the main class from the provided jar file
    if( _main != null ) return _main;
    try {
      ClassLoader h2o_ldr = getLoader();
      String dname = _key.toString(); // user_class_name.dclass
      String klass = dname.substring(0,dname.length()-7);
      Class klz = h2o_ldr.loadClass(klass);
      _usr_kls = klz;
      _main = klz.getMethod("main",String[].class);
      return _main;
    } catch( ClassNotFoundException e ) {
      return e.toString(); // Probably fails because the keyname does not match the classname
    } catch( NoSuchMethodException e ) {
      return e.toString(); // Probably fails because missing main method
    } catch( UnsupportedClassVersionError e ) {
      return e.toString(); // Probably fails because running on older JVM but compiling on newer
    }
  }

  // The top-level key for the task currently being executed
  protected static ThreadLocal<Key> _taskey = new ThreadLocal();

  // Execute a ValueCode associated with this Key (if that is what it is).
  // Return a String with any exceptions, or the methods stdout.
  public static String exec( Key obj, String[] args ) {
    Value v = DKV.get(obj);
    if( v == null ) return "ERROR: not found "+obj;
    if( !(v instanceof ValueCode) )
      return "ERROR: not H2O compiled code "+obj;
    ValueCode val = (ValueCode)v;
    // Get the main method
    Object main2 = val.get_main();
    if( main2 instanceof String ) 
      return "ERROR: main "+(String)main2;
    Method main = (Method)main2;
    // The task key about to execute
    _taskey.set(obj);

    try {
      // Buffer users' output
      Log.buffer_sys_out_err();

      main.invoke(null,(Object)args);

    } catch( IllegalAccessException e ) {
      System.err.println("ERROR: "+e); // We do not expect this post-compilation
    } catch( IllegalArgumentException e ) {
      System.err.println("ERROR: "+e); // We do not expect this post-compilation
    } catch( InvocationTargetException e ) {
      // The 'invoke' call caught any exception thrown by the target, and
      // wrapped it in an InvocationTargetException.  Peel the orginal
      // exception back out and return a printout of it.
      System.err.println("ERROR: ");
      e.getCause().printStackTrace();
    } finally {
      _taskey.set(null);
      //return "SEE FOR YOURSELF";
      return Log.unbuffer_sys_out_err().toString();
    }
  }

  // Alas, lack of a Strong-Put has me needing a spin-loop here.
  // And a spin-loop is a form of blocking, hence this class.
  private static class NeedAStrongPut implements ForkJoinPool.ManagedBlocker {
    public boolean isReleasable() { return false; }
    public boolean block() throws InterruptedException {
      Thread.sleep(10);
      return true;
    }
  }

  // Remotely execute obj.jar:clazz.map(arg)
  public static DRecursiveTask exec_map( Key jarkey, String clazz, Key keykey, int idx ) {
    // Get the array of keys, as a local array of keys
    System.out.println("Getting key "+keykey);
    Value vkeys = UKV.get(keykey);
    while( vkeys == null ) {
      System.out.println("Looking for "+keykey+" in a loop");
      // Alas, lack of a Strong-Put has me needing a spin-loop here.  Another
      // solution (besides a Strong-Put) is to have the original Key creator
      // make the Key home on itself; then all key lookups would default to the
      // creator.  As it stands, if the remote-task hits here before the Key
      // publication hits here, and the Key *homes* here - we get a Miss - even
      // though we know the key got made.
      try { H2O.FJP.managedBlock(new NeedAStrongPut()); } catch( InterruptedException e ) { }
      vkeys = UKV.get(keykey);
    }
    System.out.println("Cleared the loop...");
    byte[] buf = vkeys.get();
    int off = 0;
    int klen = UDP.get4(buf,off); off += 4;
    Key[] keys = new Key[klen];
    for( int i=0; i<klen; i++ ) {
      Key k = Key.read(buf,off);
      off += k.wire_len();
      keys[i] = k;
    }

    // Make a local instance and call map on it
    Exception e=null;
    Value v = DKV.get(jarkey);
    try {
      ValueCode val = (ValueCode)v;
      ClassLoader h2o_ldr = val.getLoader();
      Class klz = h2o_ldr.loadClass(clazz);
      Constructor<DRecursiveTask> cdt = klz.getConstructor(Key[].class,int.class,int.class,Key.class);
      DRecursiveTask dt = cdt.newInstance(keys,idx,idx+1,jarkey);
      dt.map(dt._keys[idx]);
      return dt;
    } 
    catch( ClassNotFoundException e0 ) { e=e0; }
    catch( NoSuchMethodException  e0 ) { e=e0; }
    catch( IllegalAccessException e0 ) { e=e0; }
    catch( InstantiationException e0 ) { e=e0; }
    catch( InvocationTargetException e0){e=e0; }
    catch( UnsupportedClassVersionError e0 ) { e=new Exception(e0); }
    e.printStackTrace();
    System.err.println("puking "+e);
    return null;
  }

}

/** A simple ClassLoader that verifies and distribution-compiles the given Class.
 * This class loader can return multiple classes and resolve existing H2O classes.
 */
class JarLoader extends ClassLoader {
  private final byte[] _jarbits;
  private final ConcurrentHashMap<String,Class> _classes = new ConcurrentHashMap();

  public JarLoader( byte[] jarbits ) {
    //super(init.Loader.instance()); -- I do not think this is necessary
    super(JarLoader.class.getClassLoader());    // Standard delegation model
    _jarbits = jarbits;
  }

  // no need to overload loadClass
  //public Class loadClass(String className) throws ClassNotFoundException { return findClass(className); }

  public Class findClass(String name) throws ClassNotFoundException {
    Class result = _classes.get(name);
    // did we return this class already?
    if( result!=null) return result;
    // is this class one of the H2O or System classes?
    try {  result = super.findClass(name); } // Standard delegation; system before self -- must use findClass, not load class
    catch( Exception e ) { }
    // is the class in the jar file?
    try{ if( result == null ) result = findClass0(name); }catch( Exception e ){ }
    // update the cache
    if( result != null ) {
      // Atomically update the cache.  And yes, I have seen the putIfAbsent
      // race hit, thread-pools doing class loading slam it routinely.
      Class res2 = _classes.putIfAbsent(name, result);
      return res2 == null ? result : res2;
    }
    // no luck
    throw new ClassNotFoundException(name + " not found");
  }
  
  // Load from the internal jar file
  private Class findClass0(String name) throws IOException, ClassNotFoundException {
    JarEntry je = null;
    // A stream from the jar bits (includes, e.g. unzipping)
    JarInputStream jar = new JarInputStream(new ByteArrayInputStream(_jarbits));
    String cname = name.replace(".", "/") + ".class";
    while( (je = jar.getNextJarEntry()) != null ) {
      if (je.getName().endsWith(cname)) break;
    }
    if (je == null) return null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[2048];
    while( jar.available() > 0 ){
      int read = jar.read(buffer, 0, buffer.length);
      if( read < 0 ) break;
      out.write(buffer, 0, read);
    }
    // Now we try to load it, from our pile-o-classfile bits
    Class klazz = defineClass(name, out.toByteArray(), 0, out.size());
    resolveClass(klazz);        // Also resolve it
    return klazz; 
  }
}


// A place-holder for a class-loader which "compiles" the incoming jar file
// into something suitable for parallel execution in the H2O cloud.
class CompilingClassLoader extends JarLoader {
  public CompilingClassLoader( byte[] jarbits ) {
    super(jarbits);
  }
/*  public Class findClass(String className) throws ClassNotFoundException {
    Class clz = super.findClass(className);
    // Compiling from class-1 to class-2 goes here.
    return clz;
  } */
}
