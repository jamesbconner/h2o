package test;

import org.junit.*;

import water.*;
import water.parser.SeparatedValueParser;

public class ParserTest {
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] {});
  }

  @AfterClass public static void tearDownCloud() {
//    UDPRebooted.global_kill();
  }

  private static class S {
    final String input;
    final double[][] output;

    public S(String i, double[][] o) {
      input = i;
      output = o;
    }
  }

  @Test public void testBasic() {
    S[] tests = new S[] {
        new S("1,2,3", new double[][] {
            { 1.0, 2.0, 3.0 },
        }),
    };

    for( S t : tests ) {
      Key k = Key.make();
      Value v = new Value(k, t.input);
      DKV.put(k, v);

      SeparatedValueParser p = new SeparatedValueParser(k, ',', t.output[0].length);
      int i = 0;
      for (double[] r : p) {
        Assert.assertArrayEquals(t.output[i++], r, 0.0001);
      }
    }
  }
}
