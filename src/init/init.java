package init;

import java.io.File;
import java.util.Arrays;

/** Initializer class for H2O.
 * 
 * Unpacks all the dependencies and H2O implementation from the jar file, sets
 * the loader to be able to load all the classes properly and then executes the
 * main method of the H2O class.
 * 
 * Does nothing if the H2O is not run from a jar archive. (This *is* a feature,
 * at least for the time being so that we can continue using different IDEs).
 *
 * @author peta
 */
public class init {
  public static void main(String[] args) {
    Loader loader = Loader.instance();
    if (loader.runningFromJar()) {
      loader.addInternalJar("hexbase_impl.jar");
      //System.out.println("Extracting sigar binaries...");
      loader._binlib = loader.extractInternalFolder("binlib");
      System.setProperty("org.hyperic.sigar.path",loader.binlib().getAbsolutePath());
      //System.out.println("    "+System.getProperty("org.hyperic.sigar.path"));
      //System.out.println("Extracting dependencies...");
      loader.addInternalJarFolder("sigar" ,true);
      loader.addInternalJarFolder("apache",true);
      loader.addInternalJarFolder("gson",true);
      // removed hadoop folder - pollutes path with other versions jars.
      loader.addInternalJarFolder("junit" ,true);
    } else {
      loader._binlib = new File("lib/binlib");
      //System.out.println("Not running from jar file, skipping jar extractions...");
      System.setProperty("org.hyperic.sigar.path","lib/binlib");
      //System.out.println(new File("lib/binlib").getAbsolutePath());
    }
    try {
      String cn = "water.H2O";
      if (args[0].equals("-mainClass")) {
        cn = args[1];
        args = Arrays.copyOfRange(args, 2, args.length);
      }
      Class<?> c = Class.forName(cn,true,loader);
      c.getMethod("main",String[].class).invoke(null, (Object)args);
    } catch (Exception e) {
      System.err.println("Unable to run Hexbase:");
      throw new Error(e);
    }
  }
  
}
