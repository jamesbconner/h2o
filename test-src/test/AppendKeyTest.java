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

    Key a1 = Key.make("append 1");
    new AppendKey(a1).invoke(k);
    Key[] ks = new AutoBuffer(DKV.get(k).get()).getA(Key.class);
    Assert.assertEquals(1, ks.length);
    Assert.assertEquals(a1, ks[0]);

    Key a2 = Key.make("append 2");
    new AppendKey(a2).invoke(k);
    ks = new AutoBuffer(DKV.get(k).get()).getA(Key.class);
    Assert.assertEquals(2, ks.length);
    Assert.assertEquals(a1, ks[0]);
    Assert.assertEquals(a2, ks[1]);
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

    Random r = new Random(1234567890123456789L);
    int total = 0;
    while( total < AutoBuffer.MTU*8 ) {
      byte[] kb = new byte[Key.KEY_LENGTH];
      r.nextBytes(kb);
      Key nk = Key.make(kb);
      new AppendKey(nk).invoke(k);
      v = DKV.get(k);
      byte[] vb = v.get();
      Assert.assertArrayEquals(kb, Arrays.copyOfRange(vb, vb.length-kb.length, vb.length));
      total = vb.length;
    }
    DKV.remove(k);
  }


  @Test public void testLarge() {
    doLarge(makeKey("large", false));
  }

  @Test public void testLargeRemote() {
    doLarge(makeKey("largeRemote", true));
  }
}
