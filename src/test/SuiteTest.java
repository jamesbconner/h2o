package test;

import test.KVTest;
import test.RandomForestTest;
import test.TestUtil;
import water.*;
import java.util.Arrays;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({KVTest.class})

/**
 * Created by IntelliJ IDEA.
 * User: sris
 * Date: 9/8/12
 * Time: 11:39 AM
 * Refactoring to be able to run Suite of tests
 */
public class SuiteTest {

 // static String TEST_ICE_ROOT = new String("/tmp/ice-"+System.currentTimeMillis());

  @BeforeClass static public void startLocalNode() {
    H2O.main(new String[] {});
		// "--ice_root="+TEST_ICE_ROOT});
  }

  public static Process[] TEST_JVMS = new Process[10];
  public static final Runtime RUNTIME=Runtime.getRuntime();

  private static void drainStream(PrintStream out, InputStream in) throws IOException {
    byte[] errorBytes = new byte[in.available()];
    in.read(errorBytes);
    out.print(new String(errorBytes));
  }

  public static void launch_dev_jvm(int num) {
    String[] args = new String[]{
        "java",
        "-classpath", System.getProperty("java.class.path"),
        "-Xmx512m",
        "-ea",
        "init.init",
        "-test=none",
        "-name", H2O.NAME,
        "-port", Integer.toString(H2O.WEB_PORT+3*num),
        "-ip", H2O.SELF._key._inet.getHostAddress()
			  //"-ice_root", TEST_ICE_ROOT
    };
    try {
      System.out.println("  Launching nested JVM on port "+(H2O.WEB_PORT+3*num));
      final Process P = RUNTIME.exec(args);
      RUNTIME.addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
        //  P.destroy();
        }
      }));

      TEST_JVMS[num] = P;
      while( H2O.CLOUD.size() == num ) {
        // Takes a while for the new JVM to be recognized
        try { Thread.sleep(10); } catch( InterruptedException ie ) { }
        try {
          int exitCode = P.exitValue();
          System.err.printf("Sub process died with error code %d\n", exitCode);
          System.err.println("Sub process error stream");
          drainStream(System.err, P.getErrorStream());
          System.err.println("Sub process output stream");
          drainStream(System.err, P.getInputStream());
          throw new Error("JVM died unexpectedly");
        } catch( IllegalThreadStateException e ) {
          drainStream(System.err, P.getErrorStream());
          drainStream(System.err, P.getInputStream());
        }
      }
      System.out.println("  Nested JVM joined cloud of size: "+H2O.CLOUD.size());
      if( H2O.CLOUD.size() == num+1 ) return;
      throw new Error("JVMs are dying on me");
    } catch( IOException e ) {
      System.err.println("Failed to launch nested JVM with:"+Arrays.toString(args));
    }
  }

	 // Kill excess JVMs once all testing is done
  public static void kill_test_jvms() {
    DKV.write_barrier();
    for( int i=0; i<TEST_JVMS.length; i++ ) {
      if( TEST_JVMS[i] != null ) {
        System.out.println("  Killing nested JVM on port "+(H2O.WEB_PORT+3*i));
        TEST_JVMS[i].destroy();
        TEST_JVMS[i] = null;
      }
    }
		// clean ice
		//System.out.println("Clearing ice dir");
		//deleteDir(new File(TEST_ICE_ROOT));
	}
		// Deletes all files and subdirectories under dir.
		public static boolean deleteDir(File dir) {
				if (dir.isDirectory()) {
						String[] subDir = dir.list();
						for (int i=0; i<subDir.length; i++) {
								boolean success = deleteDir(new File(dir, subDir[i]));
								if (!success) {
										return false;
								}
						}
				}
				// The directory is now empty so delete it
				return dir.delete();
	 	}

}

