package test;
import java.util.Arrays;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.exec.*;
import water.parser.ParseDataset;
import water.util.KeyUtil;

public class KeyToString {
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] { });
  }

  @AfterClass public static void checkLeakedKeys() {
  }

  @Test public void testKeyToString() {
    byte[] b = "XXXHelloAll".getBytes();
    assertTrue(Key.make(b).toString().equals("XXXHelloAll"));
    assertTrue(Arrays.equals(Key.make(b)._kb,b));
    b[0] = 16;
    b[1] = 20;
    Key k = Key.make("_202020_");
    System.err.println(k.toString());
    assertEquals(k._kb.length, 3);
    assertEquals(k._kb[0], 32);
    assertEquals(k._kb[1], 32);
    assertEquals(k._kb[2], 32);
    k = Key.make("_202020_Azaz09-.");
    assertTrue(k.toString().equals("_202020_Azaz09-."));
    k = Key.make("HiThere");
    assertTrue(k.toString().equals("HiThere"));
  }
  
}
