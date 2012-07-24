package water.test;

import java.io.IOException;
import java.util.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runner.notification.*;
import static org.junit.Assert.*;
import water.*;

public class Test {

  // A no-arg constructor for JUnit alone
  public Test() { }

  // Run some basic tests.  Create a key, test that it does not exist, insert a
  // value for it, get the value for it, delete it.
  @org.junit.Test public void test0() {
    Key k1 = Key.make("key1");
    Value v0 = DKV.get(k1);
    assertNull(v0);
    Value v1 = new Value(k1,"bits for Value1");
    DKV.put(k1,v1);
    assertEquals(v1._key,k1);
    Value v2 = DKV.get(k1);
    assertEquals(v1,v2);
    DKV.remove(k1);
    Value v3 = DKV.get(k1);
    assertNull(v3);
  }

  // Repeat test0, but with at least 3 JVMs in the Cloud
  @org.junit.Test public void test1() {
    h2o_cloud_of_size(3);
    test0();
  }

  // Spawn JVMs to make a larger cloud, up to 'cnt' JVMs
  void h2o_cloud_of_size( int cnt ) {
    int num = H2O.CLOUD.size();
    while( num < cnt ) {
      launch_dev_jvm(num);
      num = H2O.CLOUD.size();
    }
  }

  // Kill excess JVMs once all testing is done
  @AfterClass public static void kill_test_jvms() {
    for( int i=0; i<TEST_JVMS.length; i++ ) {
      if( TEST_JVMS[i] != null ) {
        System.out.println("Killing nested JVM on port "+(H2O.WEB_PORT+3*i));
        TEST_JVMS[i].destroy();
        TEST_JVMS[i] = null;
      }
    }
  }

  public static Process[] TEST_JVMS = new Process[10];
  public static final Runtime RUNTIME=Runtime.getRuntime();

  public static void launch_dev_jvm(int num) {
    String[] args = new String[]{"java","-classpath",System.getProperty("java.class.path"),"-ea","init.init","-test=none","-Xmx512m","-name",H2O.NAME,"-port",Integer.toString(H2O.WEB_PORT+3*num),"-ip",H2O.SELF._key._inet.getHostAddress()};
    try {
      System.out.println("Launching nested JVM on port "+(H2O.WEB_PORT+3*num));
      Process P = RUNTIME.exec(args);
      TEST_JVMS[num] = P;
      while( H2O.CLOUD.size() == num ) { // Takes a while for the new JVM to be recognized
        try { Thread.sleep(10); }        // sleep 10msec & test again
        catch( InterruptedException ie ) {} 
      }
      System.out.println("Nested JVM joined cloud");
      if( H2O.CLOUD.size() == num+1 ) return;
      throw new Error("JVMs are dying on me");
    } catch( IOException e ) {
      System.err.println("Failed to launch nested JVM with:"+Arrays.toString(args));
    }
  }
}
