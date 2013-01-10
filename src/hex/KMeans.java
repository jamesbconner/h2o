package hex;

import water.*;

public abstract class KMeans {

  static public double[][] run(ValueArray va, int k, double epsilon, int... cols) {
    double[][] clusters = new double[k][];
    AutoBuffer bits = va.getChunk(0);

    // Initialize clusters to first rows
    // TODO proper initialization phase
    for( int row = 0; row < clusters.length; row++ ) {
      clusters[row] = new double[cols.length];

      for( int c = 0; c < cols.length; c++ )
        clusters[row][c] = va.datad(bits, row, va._cols[cols[c]]);
    }

    for( ;; ) {
      KMeansTask task = new KMeansTask();
      task._arykey = va._key;
      task._cols = cols;
      task._clusters = clusters;
      task.invoke(va._key);
      boolean moved = false;

      for( int cluster = 0; cluster < clusters.length; cluster++ ) {
        for( int column = 0; column < cols.length; column++ ) {
          double value = task._sums[cluster][column] / task._counts[cluster];

          if( Math.abs(value - clusters[cluster][column]) > epsilon ) {
            clusters[cluster][column] = value;
            moved = true;
          }
        }
      }

      if( !moved ) break;
    }

    return clusters;
  }

  public static class KMeansTask extends MRTask {
    Key        _arykey;
    int[]      _cols;
    double[][] _clusters;

    // Sums and counts for each cluster
    double[][] _sums;
    int[]      _counts;

    @Override
    public void map(Key key) {
      assert key.home();
      ValueArray va = ValueArray.value(DKV.get(_arykey));
      AutoBuffer bits = va.getChunk(key);
      int rows = bits.remaining() / va._rowsize;
      double[] values = new double[_cols.length];

      // Create result arrays
      _sums = new double[_clusters.length][];
      _counts = new int[_clusters.length];

      for( int c = 0; c < _clusters.length; c++ )
        _sums[c] = new double[_cols.length];

      // Find closest cluster for each row
      for( int row = 0; row < rows; row++ ) {
        for( int column = 0; column < _cols.length; column++ )
          values[column] = va.datad(bits, row, va._cols[_cols[column]]);

        int cluster = closest(_clusters, values);

        // Add values and increment counter for chosen cluster
        for( int column = 0; column < _cols.length; column++ )
          _sums[cluster][column] += values[column];

        _counts[cluster]++;
      }
    }

    public static int closest(double[][] clusters, double[] point) {
      int min = 0;
      double minSqr = Double.MAX_VALUE;

      for( int cluster = 0; cluster < clusters.length; cluster++ ) {
        double sqr = 0;

        for( int column = 0; column < point.length; column++ ) {
          double delta = point[column] - clusters[cluster][column];
          sqr += delta * delta;
        }

        if( sqr < minSqr ) {
          min = cluster;
          minSqr = sqr;
        }
      }

      return min;
    }

    @Override
    public void reduce(DRemoteTask rt) {
      KMeansTask task = (KMeansTask) rt;

      for( int cluster = 0; cluster < _clusters.length; cluster++ ) {
        for( int column = 0; column < _cols.length; column++ )
          _sums[cluster][column] += task._sums[cluster][column];

        _counts[cluster] += task._counts[cluster];
      }
    }
  }
}
