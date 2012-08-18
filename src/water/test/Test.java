package water.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import water.Atomic;
import water.DKV;
import water.DRemoteTask;
import water.H2O;
import water.H2ONode;
import water.Key;
import water.UDP;
import water.UKV;
import water.Value;
import water.ValueArray;

public class Test {
  // Request that tests be "clean" on the K/V store, and restore it to the same
  // count of keys after all tests run.
  static int _initial_keycnt;

  // A no-arg constructor for JUnit alone
  public Test() { }
  
  @BeforeClass static public void startLocalNode() {
    H2O.startLocalNode();
  }

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
    System.out.println("test1");
    h2o_cloud_of_size(3);
    test0();
  }

  // ---
  // Make 100 keys, verify them all, delete them all.
  @org.junit.Test public void test2() {
    System.out.println("test2");
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
    System.out.println("test3");
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
    rbs.invoke(keys);
    assertEquals((int)((1L<<keys.length)-1),rbs._x);
  }

  // Remote Bit Set: OR together the result of a single bit-mask where the
  // shift-amount is passed in in the Key.
  @SuppressWarnings("serial")
  public static class RemoteBitSet extends DRemoteTask {
    int _x;
    public int wire_len() { return 4; }
    public int write( byte[] buf, int off ) { UDP.set4(buf,off,_x); return off+4; }
    public void write( DataOutputStream dos ) { throw new Error("unimplemented"); }
    public void read( byte[] buf, int off ) { _x = UDP.get4(buf,off);  off += 4; }
    public void read( DataInputStream dis ) { new Error("unimplemented"); }
    // Set a single bit-mask based on the shift which is passed in the Value
    public void map( Key key ) {
      assert _x == 0;                  // Never mapped into before
      Value val = DKV.get(key);        // Get the Value for the Key
      _x = 1<<(UDP.get4(val.get(),0)); // Get the shift amount, shift & set
      DKV.remove(key);                 // Remove Key when done
    }
    // OR together all results
    public void reduce( DRemoteTask rbs ) {
      _x |= ((RemoteBitSet)rbs)._x;
    }
  }

  // ---
  // Issue a large Key/Value put/get - testing the TCP path
  @org.junit.Test public void test4() {
    System.out.println("test4");
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
  // Map in h2o.jar - a multi-megabyte file - into Arraylets.
  // Run a distributed byte histogram.
  @org.junit.Test public void test5() {
    System.out.println("test5");
    h2o_cloud_of_size(3);
    String fname = "h2o.jar";
    Key h2okey = null;
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(fname);
      h2okey = ValueArray.read_put_file(fname,fis,(byte)0);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return;
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.toString());
    } finally {
      try { if( fis != null ) fis.close(); } catch( IOException e ) { }
    }
    if( h2okey == null ) fail("null h2okey");

    ByteHisto bh = new ByteHisto();
    bh.invoke(h2okey);
    int sum=0;
    for( int i=0; i<bh._x.length; i++ )
      sum += bh._x[i];
    assertEquals(new File(fname).length(),sum);

    UKV.remove(h2okey);
  }

  // Byte-wise histogram
  @SuppressWarnings("serial")
  public static class ByteHisto extends DRemoteTask {
    int _x[];
    // Count occurances of bytes
    public void map( Key key ) {
      _x = new int[256];        // One-time set histogram array
      Value val = DKV.get(key); // Get the Value for the Key
      byte[] bits = val.get();  // Compute local histogram
      for( int i=0; i<bits.length; i++ )
        _x[bits[i]&0xFF]++;
    }
    // ADD together all results
    public void reduce( DRemoteTask rbs ) {
      ByteHisto bh = (ByteHisto)rbs;
      if( _x == null ) { _x = bh._x; return; }
      for( int i=0; i<_x.length; i++ )
        _x[i] += bh._x[i];
    }

    public int wire_len() { return 1+((_x==null)?0:4*_x.length); }
    public int write( byte[] buf, int off ) {
      buf[off++] = (byte)((_x==null) ? 0 : 1);
      if( _x==null ) return off;
      for( int i=0; i<_x.length; i++ )
        off += UDP.set4(buf,off,_x[i]);
      return off;
    }
    public void write( DataOutputStream dos ) { throw new Error("unimplemented"); }
    public void read( byte[] buf, int off ) {
      int flag = buf[off++];
      if( flag == 0 ) return;
      _x = new int[256];
      for( int i=0; i<_x.length; i++ )
        _x[i] = UDP.get4(buf,(off+=4)-4);
    }
    public void read( DataInputStream dis ) { new Error("unimplemented"); }
  }

  // ---
  // Run an atomic function remotely, one time only
  @org.junit.Test public void test6() {
    System.out.println("test6");
    h2o_cloud_of_size(3);

    // Make an execution key homed to the remote node
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[0];
    if( target == H2O.SELF ) target = cloud._memary[1];
    Key key = Key.make("test6_remote",(byte)1,Key.DFJ_INTERNAL_USER,target);
    // It's a plain empty byte array - but too big for atomic update on purpose
    Value v1 = new Value(key,16);
    // Remote-put operation
    DKV.put(key,v1);
    DKV.write_barrier();

    // Atomically run this function on a clone of the bits from the existing
    // Key and install the result as the new Value.  This function may run
    // multiple times if there are collisions.
    Atomic q = new Atomic2();
    q.invoke(key);              // Run remotely; block till done
    Value val3 = DKV.get(key);
    assertNotSame(v1,val3);
    byte[] bits3 = val3.get();
    assertEquals(2,UDP.get8(bits3,0));
    assertEquals(2,UDP.get8(bits3,8));
    DKV.remove(key);            // Cleanup after test
  }

  @SuppressWarnings("serial")
  public static class Atomic2 extends Atomic {
    @Override public byte[] atomic( byte[] bits1 ) {
      long l1 = UDP.get8(bits1,0);
      long l2 = UDP.get8(bits1,8);
      l1 += 2;
      l2 += 2;
      byte[] bits2 = new byte[16];
      UDP.set8(bits2,0,l1);
      UDP.set8(bits2,8,l2);
      return bits2;
    }
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
    };
    try {
      System.out.println("  Launching nested JVM on port "+(H2O.WEB_PORT+3*num));
      final Process P = RUNTIME.exec(args);
      RUNTIME.addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          P.destroy();
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
}
