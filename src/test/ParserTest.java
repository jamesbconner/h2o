package test;

import org.junit.*;

import water.*;
import water.parser.SeparatedValueParser;

public class ParserTest {
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] {});
  }

  private double[] d(double... ds) { return ds; }
  private final double NaN = Double.NaN;

  @Test public void testBasic() {
    Object[][] t = new Object[][] {
        { "1,2,3",          d(1.0, 2.0, 3.0), },
        { "4,5,6",          d(4.0, 5.0, 6.0), },
        { "4,5.2,",         d(4.0, 5.2, NaN), },
        { ",,",             d(NaN, NaN, NaN), },
        { "asdf,qwer,1",    d(NaN, NaN, 1.0), },
        { "1.1",            d(1.1, NaN, NaN), },
        { "1.1,2.1,3.4",    d(1.1, 2.1, 3.4), },
    };
    int i = 0;
    SeparatedValueParser p;

    StringBuilder sb = new StringBuilder();
    for( i = 0; i < t.length; ++i ) sb.append(t[i][0]).append("\n");

    Key k = Key.make();
    DKV.put(k, new Value(k, sb.toString()));

    p = new SeparatedValueParser(k, ',', 3);
    i = 0;
    for( double[] r : p ) {
      Assert.assertArrayEquals((double[]) t[i++][1], r, 0.0001);
    }

    sb = new StringBuilder();
    for( i = 0; i < t.length; ++i ) sb.append(t[i][0]).append("\r\n");
    DKV.put(k, new Value(k, sb.toString()));

    p = new SeparatedValueParser(k, ',', 3);
    i = 0;
    for( double[] r : p ) {
      Assert.assertArrayEquals((double[]) t[i++][1], r, 0.0001);
    }
  }
}
