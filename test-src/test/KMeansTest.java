package test;

import static org.junit.Assert.assertEquals;
import hex.KMeans;
import hex.KMeans.KMeansTask;

import java.util.Arrays;

import org.junit.Test;

import water.*;
import water.parser.ParseDataset;

public class KMeansTest extends TestUtil {

  // @Test
  public void test1Dimension() {
    Key key = Key.make("datakey");

    try {
      ValueArray value = va_maker(key, //
          new double[] { 1.2, 5.6, 3.7, 0.6, 0.1, 2.6 });

      double[][] clusters = KMeans.run(value, 2, 1e-6, 0);

      for( int cluster = 0; cluster < clusters.length; cluster++ )
        System.out.println(cluster + ": " + Arrays.toString(clusters[cluster]));

      assertEquals(1.125, clusters[0][0], 0.000001);
      assertEquals(4.65, clusters[1][0], 0.000001);
    } finally {
      UKV.remove(key);
    }
  }

  @Test
  public void testCovtype() {
    Key fkey = load_test_file("smalldata/covtype/covtype.20k.data");
    Key okey = Key.make("covtype.hex");
    ParseDataset.parse(okey, DKV.get(fkey));
    UKV.remove(fkey);
    ValueArray va = ValueArray.value(DKV.get(okey));
    int[] cols = new int[54];

    for( int i = 0; i < cols.length; i++ )
      cols[i] = i;

    double[][] clusters = KMeans.run(va, 7, 1e-6, cols);

    AutoBuffer bits = va.getChunk(0);
    double[] values = new double[cols.length];
    int[][] confusion = new int[7][];

    for( int i = 0; i < confusion.length; i++ )
      confusion[i] = new int[7];

    for( int row = 0; row < va.numRows(); row++ ) {
      for( int column = 0; column < cols.length; column++ )
        values[column] = va.datad(bits, row, va._cols[cols[column]]);

      int cluster = KMeansTask.closest(clusters, values);
      int expected = (int) va.data(bits, row, va._cols[cols.length]);
      confusion[cluster][expected - 1]++;
    }

    for( int i = 0; i < confusion.length; i++ )
      System.out.println(Arrays.toString(confusion[i]));

    for( int cluster = 0; cluster < clusters.length; cluster++ )
      System.out.println(cluster + ": " + Arrays.toString(clusters[cluster]));

    UKV.remove(okey);
  }
}
