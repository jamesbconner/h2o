package init;


import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** Simple class loader.
 * 
 * Extends the URL class loader to simplify its support for adding new
 * resources on the go. Also is capable of extracting the resources from its
 * own jar file before loading them. 
 *
 * @author peta
 */
public class Loader extends URLClassLoader {
  
  
  File _binlib;
  
  public File binlib() {
    return _binlib;
  }
  
  // parent loader
  ClassLoader _parent;
  
  // own jar file, null if not running from a jar file
  ZipFile _ownJar;

  // singleton instance
  final static Loader _instance  = new Loader(Thread.currentThread().getContextClassLoader());
  
  final static String _os;
  final static String _ext;
  
  // Creates the loader and registers it with the current thread
  static {
    Thread.currentThread().setContextClassLoader(_instance);
    String arch = System.getProperty("sun.arch.data.model");
    String osName = System.getProperty("os.name");
    if (osName.startsWith("Windows")) {
      osName = "win";
      _ext = "dll";
    } else if (osName.equals("Linux")) {
      osName = "linux";
      _ext = "so";
    } else if (osName.equals("Mac OS X") || osName.equals("Darwin")) {
      osName = "osx";
      _ext = "dylib";
    } else {
      osName = osName;
      _ext = "so";
    }
    _os = osName + arch;
  }
  
  public static String libraryPath(String name) {
    return  new File(_instance._binlib,name+"-"+_os+"."+_ext).getAbsolutePath();
  }    
    
    
  
  /** Returns the instance of the loader.  */
  public static Loader instance() { return _instance; }
  
  // Creates the loader and determines whether it runs from a jar file or not
  private Loader(ClassLoader parent) {
    super(new URL[0]);
    _parent = parent;
    String ownJar = Loader.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    if (ownJar.endsWith(".jar")) // do not do anything if you do not run from jar
      try {
        _ownJar = new ZipFile(URLDecoder.decode(ownJar,"UTF-8"));
      } catch (IOException e) { }
  }

  /** Returns true, if the loader is running from jar.  */
  public boolean runningFromJar() { return _ownJar!=null;  }

  /** Adds existing file to the loader as a resource for classes. */
  public void addFile(File file) {
    try {
      addURL(file.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Unable to add name "+file.toString()+" to the loader. Cannot form URL.");
    }
  }
  
  /** Returns the root for the loader, which is the path where the loader's jar
   * is stored. 
   * 
   * @return Path of the loader's jar or null if not running from jar. 
   */
  public File root() {
    if (_ownJar==null) return null;
    return new File(_ownJar.getName()).getParentFile();    
  }
  
  /** Extracts the libraries from the jar file to given local path.
   * Returns the path to which they were extracted. 
   */
  File extractInternalFiles(String prefix, String to) {
    if (_ownJar==null) return null; // do nothing if we do not run from a jar file    
    Enumeration entries = _ownJar.entries();
    File extractionRoot = (to.isEmpty()) ? root() : new File(to);
    while (entries.hasMoreElements()) {
      ZipEntry e = (ZipEntry) entries.nextElement();
      String name = e.getName();
      if (!name.startsWith(prefix)) // only interested in those we want
        continue;
      File out = new File(extractionRoot,name);
      out.getParentFile().mkdirs();
      if (e.isDirectory()) // only interested in files for actual copying
        continue;
      // extract the entry
      try {
        BufferedInputStream is = new BufferedInputStream(_ownJar.getInputStream(e));
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(out));
        int blockSize;
        byte[] buffer = new byte[4096];
        while ((blockSize = is.read(buffer)) != -1)
          os.write(buffer,0,blockSize);
        os.flush();
        os.close();
        is.close();
      } catch( FileNotFoundException ex ) {
        // Expected FNF if 2 H2O instances are attempting to unpack in the same directory
      } catch (IOException ex) { System.err.println("Unable to extract file "+name+" because "+ex); 
      }
    }
    return extractionRoot;
  }
  
  
  /** Extracts single file to the root directory. */
  File extractInternalFile(String name) {
    if (_ownJar==null) // do nothing if we do not run from a jar file
      return null;
    File result = extractInternalFiles(name,"");
    result = new File(result,name);
    assert (result.exists() && !result.isDirectory());
    return result;
  }
  
  /** Extracts a folder to the root directory. */
  File extractInternalFolder(String name) {
    if (_ownJar==null) // do nothing if we do not run from a jar file
      return null;
    File result = extractInternalFiles(name,"");
    result = new File(result,name);
    assert (result.isDirectory());
    return result;
  }
  
  /** Extracts the internal jar to the root directory and adds it to the
   * classpath. 
   */
  public boolean addInternalJar(String name) {
    if (_ownJar==null) // do nothing if we do not run from a jar file
      return true;
    return addExternalJar(extractInternalFile(name));
  }
  
  /** Adds the given external jar to the classpath. 
   * 
   * @param name
   * @return 
   */
  public boolean addExternalJar(String name) {
    return addExternalJar(new File(name));
  }
  
  /** Adds the external jar file. */
  public boolean addExternalJar(File what) {
    if (!what.exists()) return false;
    addFile(what);
    return true;
  }
  
  /** Adds internal jar folder. 
   * Extracts the jar folder to the root and adds all its jars to the classpath. */
  boolean addInternalJarFolder(String name, boolean recursive) {
    return addExternalJarFolder(extractInternalFolder(name),recursive);
  }

  /** Adds all jars in the external path to the classpath.  */
  boolean addExternalJarFolder(String name, boolean recursive) {
    return addExternalJarFolder(new File(name),recursive);
  }
  
  /** Adds all jars in given external folder.
   * 
   * The jar files are added to the loader's list of resources.
   * 
   * @param dir The external folder to search for jar files to add.
   * @param recursive If recursive also all jars in the subdirectories are
   * added. 
   * @return True if all went ok, false if any errors.  
   */
  boolean addExternalJarFolder(File dir, boolean recursive) {
    boolean result = true;
    for (File f : dir.listFiles()) 
      if (f.isDirectory()) {
        if (recursive) result = addExternalJarFolder(f,true) && result;
      } else {
        if (f.getName().endsWith(".jar")) result = addExternalJar(f) && result;
      }
    return result;
  }
}