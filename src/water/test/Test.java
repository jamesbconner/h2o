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
    Value v1 = new Value(k1,"test0 bits for Value1");
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
      Value v1 = vals[i] = new Value(k,"test2 bits for Value"+i);
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
  // Issue a slew of remote puts, then issue a DFJ job on the array of keys.
  @org.junit.Test public void test3() {
    h2o_cloud_of_size(3);
    // Issue a slew of remote key puts
    Key[] keys = new Key[32];
    for( int i=0; i<keys.length; i++ ) {
      Key k = keys[i] = Key.make("key"+i);
      Value val = new Value(k,4);
      byte[] bits = val.mem();
      UDP.set4(bits,0,i);       // Each value holds a shift-count
      DKV.put(k,val);
    }
    RemoteBitSet rbs = new RemoteBitSet();
    rbs.rexec(keys);
    assertEquals(-1,rbs._x);
  }

  // Remote Bit Set: OR together the result of a single bit-mask where the
  // shift-amount is passed in in the Key.
  public static class RemoteBitSet extends DRemoteTask {
    int _x;
    public int wire_len() { return 4; }
    public int write( byte[] buf, int off ) { UDP.set4(buf,off,_x); return off+4; }
    public void write( DataOutputStream dos ) { throw new Error("unimplemented"); }
    public void read( byte[] buf, int off ) { _x = UDP.get4(buf,off);  off += 4; }
    public void read( DataInputStream dis ) { new Error("unimplemented"); }
    // Set a single bit-mask based on the shift which is passed in the Value
    public void map( Key key ) {
      Value val = DKV.get(key);        // Get the Value for the Key
      _x = 1<<(UDP.get4(val.get(),0)); // Get the shift amount, shift & set
      DKV.remove(key);                 // Remove Key when done
    }
    // OR together all results
    public void reduce( RemoteTask rbs ) {
      _x |= ((RemoteBitSet)rbs)._x;
    }
  }

  // ---
  // Issue a large Key/Value put/get - testing the TCP path
  @org.junit.Test public void test4() {
    h2o_cloud_of_size(2);

    // Make an execution key homed to the remote node
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[0];
    if( target == H2O.SELF ) target = cloud._memary[1];
    Key remote_key = Key.make("test4_remote",(byte)1,Key.DFJ_INTERNAL_USER,target); // A key homed to a specific target
    Value v0 = DKV.get(remote_key);
    assertNull(v0);
    // It's a Big Value
    Value v1 = new Value(remote_key,100000);
    byte[] bits = v1.mem();
    for( int i=0; i<bits.length; i++ )
      bits[i] = (byte)i;
    // Start the remote-put operation
    DKV.put(remote_key,v1);
    assertEquals(v1._key,remote_key);
    Value v2 = DKV.get(remote_key);
    assertEquals(v1,v2);
    DKV.remove(remote_key);
    Value v3 = DKV.get(remote_key);
    assertNull(v3);
  }


  // ---
  // Spawn JVMs to make a larger cloud, up to 'cnt' JVMs
  static public void h2o_cloud_of_size( int cnt ) {
    int num = H2O.CLOUD.size();
    while( num < cnt ) {
      launch_dev_jvm(num);
      //try { Thread.sleep(10); }        // sleep 10msec & test again
      //catch( InterruptedException ie ) {}
      num = H2O.CLOUD.size();
    }
  }

  @BeforeClass public static void record_initial_keycnt() {
    System.out.println("Running tests in Test.class");
    _initial_keycnt = H2O.store_size();
  }

  // Kill excess JVMs once all testing is done
  @AfterClass public static void kill_test_jvms() {
    DKV.write_barrier();
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
