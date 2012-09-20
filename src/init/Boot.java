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
public class Boot {
  public static void main(String[] args) {
    try {
      _init = new Boot();
      if( _init._h2oJar != null ) {
        _init.extractInternalFiles();

        File binlib = _init.internalFile("binlib");
        System.setProperty("org.hyperic.sigar.path", binlib.getAbsolutePath());
        _init.addInternalJars("hexbase_impl.jar");
        _init.addInternalJars("sigar");
        _init.addInternalJars("apache");
        _init.addInternalJars("gson");
        _init.addInternalJars("junit");
        _init.addInternalJars("asm");
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

  public static Boot _init;

  private final URLClassLoader _systemLoader;
  private final Method _addUrl;
  private final ZipFile _h2oJar;
  private final File _parentDir;
  File _binlib;

  public Boot() throws NoSuchMethodException, SecurityException, IOException {
    _systemLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
    _addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    _addUrl.setAccessible(true);

    File dir = null;
    ZipFile jar = null;
    String ownJar = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
    if( ownJar.endsWith(".jar") ) { // do nothing if not run from jar
      try {
        jar = new ZipFile(URLDecoder.decode(ownJar, "UTF-8"));
      } catch( IOException e ) { }
      dir = File.createTempFile("h2o-temp-", "");
      if( !dir.delete() ) throw new IOException("Failed to remove tmp file: " + dir.getAbsolutePath());
      if( !dir.mkdir() )  throw new IOException("Failed to create tmp dir: "  + dir.getAbsolutePath());
      dir.deleteOnExit();
    }
    _h2oJar = jar;
    _parentDir = dir;
  }

  /** Returns an external File for the internal file name. */
  public File internalFile(String name) {
    assert _parentDir != null;
    return new File(_parentDir, name);
  }

  /** Add a jar to the system classloader */
  public void addInternalJars(String name) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    addExternalJars(internalFile(name));
  }

  /** Adds all jars in given directory to the classpath. */
  public void addExternalJars(File file) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    assert file.exists();
    if( file.isDirectory() ) {
      for( File f : file.listFiles() ) addExternalJars(f);
    } else if( file.getName().endsWith(".jar") ) {
      _addUrl.invoke(_systemLoader, file.toURI().toURL());
    }
  }

  /** Extracts the libraries from the jar file to given local path.
   * Returns the path to which they were extracted.
   */
  private void extractInternalFiles() {
    assert _h2oJar != null;
    Enumeration entries = _h2oJar.entries();
    while( entries.hasMoreElements() ) {
      ZipEntry e = (ZipEntry) entries.nextElement();
      String name = e.getName();
      File out = internalFile(name);
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
