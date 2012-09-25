package test;
import java.util.Arrays;
import java.util.Random;

import org.junit.*;

import water.*;

public class AppendKeyTest {
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < 10000) {
      if (H2O.CLOUD.size() > 1) break;
      try { Thread.sleep(100); } catch( InterruptedException ie ) {}
    }
    Assert.assertTrue(H2O.CLOUD.size() > 1);
  }

  private int _initialKeyCount;

  @Before public void setupLeakedKeys() {
    _initialKeyCount = H2O.store_size();
  }

  @After public void checkLeakedKeys() {
    int leaked_keys = H2O.store_size() - _initialKeyCount;
    Assert.assertEquals("No keys leaked", 0, leaked_keys);
  }

  public Key makeKey(String n, boolean remote) {
    if(!remote) return Key.make(n);
    H2O cloud = H2O.CLOUD;
    H2ONode target = cloud._memary[0];
    if( target == H2O.SELF ) target = cloud._memary[1];
    return Key.make(n,(byte)1,Key.DFJ_INTERNAL_USER,target);
  }

  private void doBasic(Key k) {
    Value v = DKV.get(k);
    Assert.assertNull(v);

    new AppendKey("append 1".getBytes()).invoke(k);
    v = DKV.get(k);
    byte[] b = v.get();
    Assert.assertEquals(1, UDP.get4(b, 0));
    Assert.assertEquals("append 1", new String(b, 4, b.length-4));

    new AppendKey("append 2".getBytes()).invoke(k);
    v = DKV.get(k);
    b = v.get();
    Assert.assertEquals(2, UDP.get4(b, 0));
    Assert.assertEquals("append 1append 2", new String(b, 4, b.length-4));
    DKV.remove(k);
  }

  @Test public void testBasic() {
    doBasic(makeKey("basic", false));
  }

  @Test public void testBasicRemote() {
    doBasic(makeKey("basicRemote", true));
  }

  private void doLarge(Key k) {
    Value v = DKV.get(k);
    Assert.assertNull(v);
    Random r = new Random(0);
    byte[] a1 = new byte[MultiCast.MTU * 2];
    r.nextBytes(a1);
    new AppendKey(a1).invoke(k);
    v = DKV.get(k);
    byte[] b = v.get();
    Assert.assertEquals(1, UDP.get4(b, 0));
    Assert.assertArrayEquals(a1, Arrays.copyOfRange(b, 4, 4+a1.length));

    byte[] a2 = new byte[MultiCast.MTU * 4];
    r.nextBytes(a2);
    new AppendKey(a2).invoke(k);
    v = DKV.get(k);
    b = v.get();
    Assert.assertEquals(2, UDP.get4(b, 0));
    Assert.assertArrayEquals(a1, Arrays.copyOfRange(b, 4, 4+a1.length));
    Assert.assertArrayEquals(a2, Arrays.copyOfRange(b, 4+a1.length, 4+a1.length+a2.length));
    DKV.remove(k);
  }


  @Test public void testLarge() {
    doLarge(makeKey("large", false));
  }

  @Test public void testLargeRemote() {
    doLarge(makeKey("largeRemote", true));
  }
}
