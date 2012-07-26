package water.test;

import java.io.*;
import java.util.*;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runner.notification.*;
import static org.junit.Assert.*;
import water.*;

public class Test {
  // Request that tests be "clean" on the K/V store, and restore it to the same
  // count of keys after all tests run.
  static int _initial_keycnt;


  // A no-arg constructor for JUnit alone
  public Test() { }

  // ---
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

  // ---
  // Repeat test0, but with at least 3 JVMs in the Cloud
  @org.junit.Test public void test1() {
    h2o_cloud_of_size(3);
    test0();
  }

  // ---
  // Make 100 keys, verify them all, delete them all.
  @org.junit.Test public void test2() {
    h2o_cloud_of_size(3);
    Key   keys[] = new Key  [100];
    Value vals[] = new Value[keys.length];
    for( int i=0; i<keys.length; i++ ) {
      Key k = keys[i] = Key.make("key"+i);
      Value v0 = DKV.get(k);
      assertNull(v0);
      Value v1 = vals[i] = new Value(k,"bits for Value"+i);
      DKV.put(k,v1);
      assertEquals(v1._key,k);
    }
    for( int i=0; i<keys.length; i++ ) {
      Value v = DKV.get(keys[i]);
      assertEquals(vals[i],v);
    }
    for( int i=0; i<keys.length; i++ ) {
      DKV.remove(keys[i]);
    }
    for( int i=0; i<keys.length; i++ ) {
      Value v3 = DKV.get(keys[i]);
      assertNull(v3);
    }
  }

  // ---
  // Make a remote Node do a put, and we do a get & confirm we can see it.
  @org.junit.Test public void test3() throws FileNotFoundException, IOException {
    h2o_cloud_of_size(2);
    H2O cloud = H2O.CLOUD;
  
    // Make an execution key homed to the remote node
    H2ONode target = cloud._memary[0];
    if( target == H2O.SELF ) target = cloud._memary[1];
    Key remote_key = Key.make("test3_remote",(byte)1,Key.DFJ_INTERNAL_USER,target); // A key homed to a specific target
    // Put it to the cloud
    Value v1 = new Value(remote_key,"bits for remote_key");
    DKV.put(remote_key,v1);
    // Have the remote node run stuff
    RemoteTest3 rt3 = new RemoteTest3(remote_key);
    rt3.compute();              // Run stuff remotely
    assertEquals(123,rt3._cookie);
    // We should now see the key made by the remote task
    Key local_key = Key.make("test3_local");
    Value v2 = DKV.get(local_key);
    assertNotNull(v2);
    byte[] v2bits = v2.get();
    assertArrayEquals("bits for local_key".getBytes(),v2bits);
  
    DKV.remove(remote_key);
    DKV.remove( local_key);
    DKV.remove(rt3._keykey);
  }

  public static class RemoteTest3 extends DRecursiveTask {
    int _cookie;
    // This constructer is used *locally* to build an initial task
    RemoteTest3( Key remote_key ) {
      super(new Key[]{remote_key},Key.make("hexbase_impl.jar",(byte)1,Key.DEV_JAR));
    }
    // This constructor is run *remotely*, to make a remote DRecursiveTask.
    public RemoteTest3( Key[] keys, int lo, int hi, Key jarkey ) { super(keys,lo,hi,jarkey); }

    // User overrides this to reduce 2 of his answers to 1 of his answers
    public DRecursiveTask reduce( DRecursiveTask d ) { return null; }
    // User overrides this to convert a Key to an answer, stored in 'this'
    public void map( Key k ) {
      assertEquals(new String(k._kb,2+H2ONode.wire_len(),12),"test3_remote");
      Value v1 = DKV.get(k);
      byte[] v1bits = v1.get();
      assertArrayEquals(v1bits,"bits for remote_key".getBytes());
      Key local_key = Key.make("test3_local");
      Value v2 = new Value(local_key,"bits for local_key");
      DKV.put(local_key,v2);
      _cookie = 123;
    }
    // User overrides these methods to send his results back and forth.
    // Reads & writes user-guts to a line-wire format on a correctly typed object
    protected int wire_len() { return 4; }
    protected void read( byte[] buf, int off ) { _cookie = UDP.get4(buf,off); }
    protected void read( DataInputStream dis ) { throw new Error("unimplemented"); }
    protected void write( byte[] buf, int off ) { UDP.set4(buf,off,_cookie); }
    protected void write( DataOutputStream dos ) { throw new Error("unimplemented"); }
  }

  // ---
  // Spawn JVMs to make a larger cloud, up to 'cnt' JVMs
  static public void h2o_cloud_of_size( int cnt ) {
    int num = H2O.CLOUD.size();
    while( num < cnt ) {
      launch_dev_jvm(num);
      num = H2O.CLOUD.size();
    }
  }

  @BeforeClass public static void record_initial_keycnt() {
    System.out.println("Running tests in Test.class");
    _initial_keycnt = H2O.store_size();
  }

  // Kill excess JVMs once all testing is done
  @AfterClass public static void kill_test_jvms() {
    for( int i=0; i<TEST_JVMS.length; i++ ) {
      if( TEST_JVMS[i] != null ) {
        System.out.println("  Killing nested JVM on port "+(H2O.WEB_PORT+3*i));
        TEST_JVMS[i].destroy();
        TEST_JVMS[i] = null;
      }
    }

    int leaked_keys = H2O.store_size() - _initial_keycnt;
    if( leaked_keys > 0 )
      System.err.println("Tests leaked "+leaked_keys+" keys");
    System.out.println("Done testing Test.class");
  }

  public static Process[] TEST_JVMS = new Process[10];
  public static final Runtime RUNTIME=Runtime.getRuntime();

  public static void launch_dev_jvm(int num) {
    String[] args = new String[]{"java","-classpath",System.getProperty("java.class.path"),"-ea","init.init","-test=none","-Xmx512m","-name",H2O.NAME,"-port",Integer.toString(H2O.WEB_PORT+3*num),"-ip",H2O.SELF._key._inet.getHostAddress()};
    try {
      System.out.println("  Launching nested JVM on port "+(H2O.WEB_PORT+3*num));
      Process P = RUNTIME.exec(args);
      TEST_JVMS[num] = P;
      while( H2O.CLOUD.size() == num ) { // Takes a while for the new JVM to be recognized
        try { Thread.sleep(10); }        // sleep 10msec & test again
        catch( InterruptedException ie ) {}
      }
      System.out.println("  Nested JVM joined cloud");
      if( H2O.CLOUD.size() == num+1 ) return;
      throw new Error("JVMs are dying on me");
    } catch( IOException e ) {
      System.err.println("Failed to launch nested JVM with:"+Arrays.toString(args));
    }
  }
}
