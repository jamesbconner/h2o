package test;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import water.DKV;
import water.H2O;
import water.Key;
import water.Value;
import water.ValueArray;
import water.parser.ParseDataset.ColumnDomain;
import water.parser.SeparatedValueParser;
import water.parser.SeparatedValueParser.Row;

public class ParserTest {
  @BeforeClass public static void setupCloud() {
    H2O.main(new String[] {});
  }

  private double[] d(double... ds) { return ds; }
  private String[] s(String...ss)  { return ss; }
  private final double NaN = Double.NaN;
  private final char[] SEPARATORS = new char[] {',', ' '};

  private Key[] k(String... data) {
    Key[] keys = new Key[data.length];
    Key k = Key.make();
    ValueArray va = new ValueArray(k, data.length << ValueArray.LOG_CHK, Value.ICE);
    DKV.put(k, va);
    for (int i = 0; i < data.length; ++i) {
      keys[i] = va.make_chunkkey(i << ValueArray.LOG_CHK);
      DKV.put(keys[i], new Value(keys[i], data[i]));
    }
    return keys;
  }

  @Test public void testBasic() {
    String[] data = new String[] {
        "1|2|3",
        "4|5|6",
        "4|5.2|",
        "||",
        "asdf|qwer|1",
        "1.1",
        "1.1|2.1|3.4",
    };

    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, NaN),
        d(NaN, NaN, 1.0),
        d(1.1, NaN, NaN),
        d(1.1, 2.1, 3.4),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);

      int i = 0;
      SeparatedValueParser p;

      StringBuilder sb = new StringBuilder();
      for( i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");

      Key k = Key.make();
      DKV.put(k, new Value(k, sb.toString()));

      p = new SeparatedValueParser(k, separator, 3);
      i = 0;
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }

      sb = new StringBuilder();
      for( i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\r\n");
      DKV.put(k, new Value(k, sb.toString()));

      p = new SeparatedValueParser(k, separator, 3);
      i = 0;
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }
    }
  }

  @Test public void testChunkBoundaries() {
    String[] data = new String[] {
        "1|2", "|3\n",
        "2|3|", "4\n",
        "3|4|5"
    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(2.0, 3.0, 4.0),
        d(3.0, 4.0, 5.0),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key[] keys = k(dataset);

      int i = 0;
      SeparatedValueParser p;

      p = new SeparatedValueParser(keys[0], separator, 3);
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }
      Assert.assertEquals(1, i);
      p = new SeparatedValueParser(keys[1], separator, 3);
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }
      Assert.assertEquals(2, i);
      p = new SeparatedValueParser(keys[2], separator, 3);
      for( Row r : p ) {
        Assert.fail("Key 2 should have skipped the record: 2,3,4 got " + r);
      }
      Assert.assertEquals(2, i);
      p = new SeparatedValueParser(keys[3], separator, 3);
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }
      Assert.assertEquals(3, i);
      Assert.assertEquals(3, exp.length);
      p = new SeparatedValueParser(keys[4], separator, 3);
      for( Row r : p ) {
        Assert.fail("Key 4 should have skipped the record: 3,4,5 got " + r);
      }
      Assert.assertEquals(5, keys.length);
    }
  }

  @Test public void testChunkBoundariesMixedLineEndings() {
    String[] data = new String[] {
        "1|2", "|3\n",
        "2|3|", "4\n",
        "3|4|5\r\n",
        "4|5|6", "\r\n",
        "5", "|6|", "7\r\n",
        "6|7|8\r\n" +
        "7|8|9\r\n" +
        "8|9", "|10\r\n" +
        "9|10|11\n" +
        "10|11|12",
        "\n11|12|13", "\n" +
        "12|13|14\n" +
        "13|14|15\n" +
        "14|15|16\r", "\n" +
        "15|16|17\n" +
        "16|17|18"
    };
    double[][] exp = new double[][] {
        d(1,  2,  3),
        d(2,  3,  4),
        d(3,  4,  5),
        d(4,  5,  6),
        d(5,  6,  7),
        d(6,  7,  8),
        d(7,  8,  9),
        d(8,  9,  10),
        d(9,  10, 11),
        d(10, 11, 12),
        d(11, 12, 13),
        d(12, 13, 14),
        d(13, 14, 15),
        d(14, 15, 16),
        d(15, 16, 17),
        d(16, 17, 18),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);

      Key[] keys = k(dataset);

      int i = 0;
      SeparatedValueParser p;
      for( Key k : keys ) {
        p = new SeparatedValueParser(k, separator, 3);
        for( Row r : p ) {
          Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
        }
      }
      Assert.assertEquals(exp.length, i);
    }
  }

  @Test public void testNondecimalColumns() {
    String data[] = {
        "1|  2|one\n",
        "3|  4|two\n",
        "5|  6|three\n",
        "7|  8|one\n",
        "9| 10|two\n",
        "11|12|three\n",
        "13|14|one\n",
        "15|16|\"two\"\n",
        "17|18|\" four\"\n",
        "19|20| three  \n",
    };
    double[][] expDouble = new double[][] {
        d(1,  2, NaN), // preserve order
        d(3,  4, NaN),
        d(5,  6, NaN),
        d(7,  8, NaN),
        d(9, 10, NaN),
        d(11,12, NaN),
        d(13,14, NaN),
        d(15,16, NaN),
        d(17,18, NaN),
        d(19,20, NaN),
    };

    String[][] expString = new String[][] {
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, " four"),
        s(null,null, "three"),
    };

    String expDomain[] = s( "one", "two", "three", " four" );

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key[] keys = k(dataset);

      SeparatedValueParser p;
      ColumnDomain cds[] = new ColumnDomain[3];
      for (int j = 0; j < 3; j++) cds[j] = new ColumnDomain();
      int i = 0;
      for( Key k : keys ) {
        p = new SeparatedValueParser(k, separator, 3, cds);
        for( Row r : p ) {
          Assert.assertArrayEquals(expDouble[i], r._fieldVals, 0.0001);
          Assert.assertArrayEquals(expString[i], r._fieldStringVals);
          i++;
        }
      }
      Assert.assertTrue ( cds[0].size() == 0 );
      Assert.assertTrue ( cds[1].size() == 0 );
      Assert.assertTrue ( cds[2].size() != 0 );
      Assert.assertArrayEquals(expDomain, cds[2].toArray());
    }
  }

  @Test public void testMultipleNondecimalColumns() {
    String data[] = {
        "foo|    2|one\n",
        "bar|    4|two\n",
        "foo|    6|three\n",
        "bar|    8|one\n",
        "bar|ten|two\n",
        "bar|   12|three\n",
        "foobar|14|one\n",
    };
    double[][] expDouble = new double[][] {
        d(NaN,   2, NaN), // preserve order
        d(NaN,   4, NaN),
        d(NaN,   6, NaN),
        d(NaN,   8, NaN),
        d(NaN, NaN, NaN),
        d(NaN,  12, NaN),
        d(NaN,  14, NaN),
    };

    String[][] expString = new String[][] {
        s("foo",   null, "one"),
        s("bar",   null, "two"),
        s("foo",   null, "three"),
        s("bar",   null, "one"),
        s("bar",  "ten", "two"),
        s("bar",   null, "three"),
        s("foobar",null, "one"),
    };

    String expDomain[][] = new String[][] {
        s( "foo", "bar", "foobar"),
        s( "ten"),
        s( "one", "two", "three" ),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key[]    keys = k(dataset);

      SeparatedValueParser p;
      ColumnDomain cds[] = new ColumnDomain[3];
      for (int j = 0; j < 3; j++) cds[j] = new ColumnDomain();
      int i = 0;
      for( Key k : keys ) {
        p = new SeparatedValueParser(k, separator, 3, cds);
        for( Row r : p ) {
          Assert.assertArrayEquals(expDouble[i], r._fieldVals, 0.0001);
          Assert.assertArrayEquals(expString[i], r._fieldStringVals);
          i++;
        }
      }

      for (int j = 0; j < 3; j++)
        Assert.assertArrayEquals(expDomain[j], cds[j].toArray());
    }
  }



  @Test public void testBasicSpaceAsSeparator() {

    String[] data = new String[] {
        "   1|2|3",
        "  4  |   5  |     6",
        "4|5.2 ",
        " ",
        "asdf|qwer|1",
        "1.1",
        "1.1|2.1|3.4",
    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, NaN),
        d(NaN, NaN, 1.0),
        d(1.1, NaN, NaN),
        d(1.1, 2.1, 3.4),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);

      int i = 0;
      StringBuilder sb = new StringBuilder();
      for( i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");

      Key k = Key.make();
      DKV.put(k, new Value(k, sb.toString()));

      SeparatedValueParser p;

      i = 0;
      p = new SeparatedValueParser(k, separator, 3);
      for( Row r : p ) {
        Assert.assertArrayEquals(exp[i++], r._fieldVals, 0.0001);
      }
    }
  }

  String[] getDataForSeparator(char sep, String[] data) {
    return getDataForSeparator('|', sep, data);
  }
  String[] getDataForSeparator(char placeholder, char sep, String[] data) {
    String[] result = new String[data.length];
    for (int i = 0; i < data.length; i++) {
      result[i] = data[i].replace(placeholder, sep);
    }
    return result;
  }
}
