package init;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.util.Arrays;


/** Initializer class for H2O.
 *
 * Unpacks all the dependencies and H2O implementation from the jar file, sets
 * the loader to be able to load all the classes properly and then executes the
 * main method of the H2O class.
 *
 * Does nothing if the H2O is not run from a jar archive. (This *is* a feature,
 * at least for the time being so that we can continue using different IDEs).
 */
public class init {
  public static void main(String[] args) {
    try {
      Loader loader = Loader.instance();
      if( loader.runningFromJar() ) {
        File impl = loader.addInternalJar("hexbase_impl.jar");
        loader._binlib = loader.extractInternalFolder("binlib");
        System.setProperty("org.hyperic.sigar.path" ,loader._binlib.getAbsolutePath());
        loader.addInternalJarFolder("sigar",  true);
        loader.addInternalJarFolder("apache", true);
        loader.addInternalJarFolder("gson",   true);
        loader.addInternalJarFolder("junit",  true);
        loader.addInternalJarFolder("asm",    true);

        File asmJar = loader.extractInternalFile("asm/asm-all-4.0.jar");
        URLClassLoader system = (URLClassLoader) ClassLoader.getSystemClassLoader();
        loadJar(system, asmJar);

        // if this becomes to ghetto, we can repackage lib/tools.jar
        loader.addExternalJar(System.getProperty("java.home")+"/../lib/tools.jar");
        loadVmAgent(loader, impl);
      } else {
        loader._binlib = new File("lib/binlib");
        System.setProperty("org.hyperic.sigar.path","lib/binlib");
      }
      String cn = "water.H2O";
      if( args.length >= 2 && args[0].equals("-mainClass") ) {
        cn = args[1];
        args = Arrays.copyOfRange(args, 2, args.length);
      }
      Class<?> c = Class.forName(cn,true,loader);
      c.getMethod("main",String[].class).invoke(null, (Object)args);
    } catch( Exception e ) {
      System.err.println("Unable to run Hexbase:");
      throw new Error(e);
    }
  }

  private static void loadJar(URLClassLoader cl, File file) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, MalformedURLException {
    Method addUrl = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
    addUrl.setAccessible(true);
    addUrl.invoke(cl, file.toURI().toURL());
  }

  private static void loadVmAgent(Loader loader, File f) {
    String nameOfRunningVM = ManagementFactory.getRuntimeMXBean().getName();
    int p = nameOfRunningVM.indexOf('@');
    String pid = nameOfRunningVM.substring(0, p);
    try {
      // import com.sun.tools.attach.VirtualMachine;
      // VirtualMachine vm = VirtualMachine.attach(pid);
      // vm.loadAgent(jarFilePath, "");
      // vm.detach();

      Class<?> vmC = Class.forName("com.sun.tools.attach.VirtualMachine", true, loader);
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
}
