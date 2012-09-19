package init;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


/** Initializer class for H2O.
 *
 * Unpacks all the dependencies and H2O implementation from the jar file, sets
 * the loader to be able to load all the classes properly and then executes the
 * main method of the H2O class.
 *
 * Does nothing if the H2O is not run from a jar archive. (This *is* a feature,
 * at least for the time being so that we can continue using different IDEs).
 */
public class Init {
  public static void main(String[] args) {
    try {
      _init = new Init();
      if( _init._h2oJar != null ) {
        File binlib = _init.extractInternalFolder("binlib");
        System.setProperty("org.hyperic.sigar.path", binlib.getAbsolutePath());

        _init.addInternalJar("hexbase_impl.jar");
        _init.addInternalJarFolder("sigar");
        _init.addInternalJarFolder("apache");
        _init.addInternalJarFolder("gson");
        _init.addInternalJarFolder("junit");
        _init.addInternalJarFolder("asm");

        // if this becomes to ghetto, we can repackage lib/tools.jar
//        loader.addExternalJar(System.getProperty("java.home")+"/../lib/tools.jar");
//        loadVmAgent(loader, impl);
      } else {
        System.setProperty("org.hyperic.sigar.path","lib/binlib");
      }

      String mainClass = "water.H2O";
      if( args.length >= 2 && args[0].equals("-mainClass") ) {
        mainClass = args[1];
        args = Arrays.copyOfRange(args, 2, args.length);
      }
      Class<?> c = Class.forName(mainClass);
      c.getMethod("main",String[].class).invoke(null, (Object)args);
    } catch( Exception e ) {
      System.err.println("Unable to run Hexbase:");
      throw new Error(e);
    }
  }

  public static Init _init;

  private final URLClassLoader _systemLoader;
  private final Method _addUrl;
  private final ZipFile _h2oJar;
  File _binlib;

  public Init() throws NoSuchMethodException, SecurityException {
    _systemLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    _addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    _addUrl.setAccessible(true);

    ZipFile jar = null;
    String ownJar = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    if( ownJar.endsWith(".jar") ) { // do nothing if not run from jar
      try {
        jar = new ZipFile(URLDecoder.decode(ownJar, "UTF-8"));
      } catch( IOException e ) { }
    }
    _h2oJar = jar;
  }

  /** Returns the root for the loader, which is the path where the loader's jar
   * is stored or null if not running from a jar. */
  public File root() {
    if( _h2oJar == null ) return null;
    return new File(_h2oJar.getName()).getParentFile();
  }

  /** Extracts a jar folder to the root and its jars to the classpath. */
  public boolean addInternalJarFolder(String name) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    return addExternalJarFolder(extractInternalFolder(name));
  }

  /** Extracts jar to the root and add it to the classpath.  */
  public File addInternalJar(String name) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    if( _h2oJar == null ) return null;
    return addExternalJar(extractInternalFile(name));
  }

  /** Extracts single file to the root directory. */
  File extractInternalFile(String name) {
    if( _h2oJar == null ) return null;
    File result = extractInternalFiles(name);
    result = new File(result,name);
    assert result.exists() && !result.isDirectory();
    return result;
  }



  /** Adds all jars in given external folder.
   *
   * The jar files are added to the loader's list of resources.
   *
   * @param dir The external folder to search for jar files to add.
   * @return True if all went ok, false if any errors.
   */
  public boolean addExternalJarFolder(File dir) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    boolean result = true;
    for( File f : dir.listFiles() ) {
      if( f.isDirectory() ) {
        result &= addExternalJarFolder(f);
      } else {
        if( f.getName().endsWith(".jar") ) result &= addExternalJar(f) != null;
      }
    }
    return result;
  }

  public File addExternalJar(File what) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    if( !what.exists() ) return null;
    _addUrl.invoke(_systemLoader, what.toURI().toURL());
    return what;
  }

  /** Extracts a folder to the root directory. */
  private File extractInternalFolder(String name) {
    if( _h2oJar == null ) return null;
    File result = extractInternalFiles(name);
    result = new File(result,name);
    assert result.isDirectory();
    return result;
  }

  /** Extracts the libraries from the jar file to given local path.
   * Returns the path to which they were extracted.
   */
  private File extractInternalFiles(String prefix) {
    if( _h2oJar == null ) return null;
    Enumeration entries = _h2oJar.entries();
    File extractionRoot = root();
    while( entries.hasMoreElements() ) {
      ZipEntry e = (ZipEntry) entries.nextElement();
      String name = e.getName();
      if( !name.startsWith(prefix) ) continue;

      File out = new File(extractionRoot,name);
      out.getParentFile().mkdirs();
      if( e.isDirectory() ) continue; // mkdirs() will handle these

      // extract the entry
      try {
        BufferedInputStream is = new BufferedInputStream(_h2oJar.getInputStream(e));
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(out));
        int read;
        byte[] buffer = new byte[4096];
        while( (read = is.read(buffer)) != -1 ) os.write(buffer,0,read);
        os.flush();
        os.close();
        is.close();
      } catch( FileNotFoundException ex ) {
        // Expected FNF if 2 H2O instances are attempting to unpack in the same directory
      } catch( IOException ex ) {
        System.err.println("Unable to extract file "+name+" because "+ex);
      }
    }
    return extractionRoot;
  }

  private void loadVmAgent(File f) {
    String nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName();
    int p = nameOfRunningVM.indexOf('@');
    String pid = nameOfRunningVM.substring(0, p);
    try {
      // import com.sun.tools.attach.VirtualMachine;
      // VirtualMachine vm = VirtualMachine.attach(pid);
      // vm.loadAgent(jarFilePath, "");
      // vm.detach();

      Class<?> vmC = Class.forName("com.sun.tools.attach.VirtualMachine");
      Method attach = vmC.getMethod("attach", String.class);
      Method detach = vmC.getMethod("detach");
      Method loadAgent = vmC.getMethod("loadAgent", String.class, String.class);

      Object vm = attach.invoke(null, pid);
      loadAgent.invoke(vm, f.getAbsolutePath(), "");
      detach.invoke(vm);
    } catch( Exception e ) {
      System.err.println("Unable to register VM agent:");
      throw new Error(e);
    }
  }

  public InputStream getResource(String uri) {
    if( _h2oJar != null ) {
      return _systemLoader.getResourceAsStream("resources"+uri);
    } else { // to allow us to read things not only from the loader
      try {
        return new FileInputStream(new File("lib/resources"+uri));
      } catch (FileNotFoundException e) {
        return null;
      }
    }
  }
}
