package test;
import static org.junit.Assert.*;
import hex.LinearRegression;

import java.io.File;

import org.junit.*;

import water.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;

public class KVTest {
  private static int _initial_keycnt = 0;

  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      if (H2O.CLOUD.size() > 2) break;
      try { Thread.sleep(100); } catch( InterruptedException ie ) {}
    }
    assertEquals("Cloud size of 3", 3, H2O.CLOUD.size());
    _initial_keycnt = H2O.store_size();
  }

  @AfterClass public static void checkLeakedKeys() {
    int leaked_keys = H2O.store_size() - _initial_keycnt;
    assertEquals("No keys leaked", 0, leaked_keys);
  }

  // ---
  // Run some basic tests.  Create a key, test that it does not exist, insert a
  // value for it, get the value for it, delete it.
  @Test public void testBasicCRUD() {
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
  // Make 100 keys, verify them all, delete them all.
  @Test public void test100Keys() {
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
  @Test public void testRemoteBitSet() {
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
  public static class RemoteBitSet extends MRTask {
    // Set a single bit-mask based on the shift which is passed in the Value
    private int _x;
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
  @Test public void testTcpCRUD() {
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
  @Test public void testMultiMbFile() throws Exception {
    File file = KeyUtil.find_test_file("h2o.jar");
    Key h2okey = KeyUtil.load_test_file(file);
    ByteHisto bh = new ByteHisto();
    bh.invoke(h2okey);
    int sum=0;
    for( int i=0; i<bh._x.length; i++ )
      sum += bh._x[i];
    assertEquals(file.length(),sum);

    UKV.remove(h2okey);
  }

  // Byte-wise histogram
  public static class ByteHisto extends MRTask {
    int[] _x;

    // Count occurrences of bytes
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
  }

  // ---
  // Run an atomic function remotely, one time only
  @Test public void testRemoteAtomic() {
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
  // Test parsing "cars.csv" and running LinearRegression
  @Test public void testLinearRegression() {
    Key fkey = KeyUtil.load_test_file("smalldata/cars.csv");
    Key okey = Key.make("cars.hex");
    ParseDataset.parse(okey,DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray va = (ValueArray)DKV.get(okey);
    // Compute LinearRegression between columns 2 & 3
    String LR_result = LinearRegression.run(va,2,3);
    String[] res = LR_result.split("<p>");
    assertEquals("Linear Regression of cars.hex between columns cylinders and displacement (cc)",res[0]);
    //assertEquals("Pass 1 in 10msec",res[1]);
    //assertEquals("Pass 2 in 6msec",res[2]);
    assertEquals("<b>y = 58.326241377521995 * x + -124.57816399564385</b>",res[3]);
    //assertEquals("Pass 3 in 6msec",res[4]);
    assertEquals("R^2                 = 0.9058985668996267",res[5]);
    assertEquals("std error of beta_1 = 0.9352584499359637",res[6]);
    UKV.remove(okey);
  }
}
