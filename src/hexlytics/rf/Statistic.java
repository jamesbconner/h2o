package hexlytics.rf;

import hexlytics.rf.Data.Row;

import java.util.Arrays;
import java.util.Random;

/** A general statistic framework. Keeps track of the column distributions and
 * analyzes the column splits in the end producing the single split that will
 * be used for the node.
 */
abstract class Statistic {
  protected final int[][][] _columnDists;  // Column distributions for the given statistic
  protected final int[] _features;         // Columns/features that are currently used.
  protected final Random random;             // pseudo random number generator
  
  /** Returns the best split for a given column   */
  protected abstract Split columnSplit    (int colIndex, Data d, int[] dist, int distWeight);
  protected abstract Split columnExclusion(int colIndex, Data d, int[] dist, int distWeight);

  /** Split descriptor for a particular column.
   *
   * Holds the column name and the split point, which is the last column class
   * that will go to the left tree. If the column index is -1 then the split
   * value indicates the return value of the node.
   */
  static class Split {
    final int _column;
    final int _split;
    final double _fitness;
    
    Split(int column, int split, double fitness) {
      _column = column;  _split = split;  _fitness = fitness;
    }

    /** A constant split used for true leaf nodes where all rows are of the same class.  */
    static Split constant(int result) {  return new Split(-1, result, -1); }

    /** An impossible split, which behaves like a constant split. However impossible split
     * occurs when there are different row classes present, but they all have
     * the same column value and therefore no split can be made.
     */
    static Split impossible(int result) { return new Split(-2, result, -1);  }

    /** Classic split. All lower or equal than split value go left, all greater go right.  */
    static Split split(int column, int split, double fitness) { return new Split(column, split,fitness); }

    /** Exclusion split. All equal to split value go left, all different go right.  */
    static Split exclusion(int column, int split, double fitness) { return new ExclusionSplit(column,split,fitness); }

    final boolean isLeafNode()   { return _column < 0; }
    final boolean isConstant()   { return _column == -1; }
    final boolean isImpossible() { return _column == -2;  }
    final boolean betterThan(Split other) { return _fitness > other._fitness; }
    final boolean isExclusion()  { return this instanceof ExclusionSplit; }
  }

  /** An exclusion split.  */
  static class ExclusionSplit extends Split {
    ExclusionSplit(int column, int split, double fitness) {  super(column, split,fitness);  }
  }

  /** Aggregates the given column's distribution to the provided array and
   * returns the sum of weights of that array.  */
  private final int aggregateColumn(int colIndex, int[] dist) {
    int sum = 0;
    for (int j = 0; j < _columnDists[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        int tmp = _columnDists[colIndex][j][i];
        sum     += tmp;
        dist[i] += tmp;
      }
    }
    return sum;
  }

  Statistic(Data data, int features, int seed) {
    random = new Random(seed);
    // first create the column distributions
    _columnDists = new int[data.columns()][][];
    for (int i = 0; i < _columnDists.length; ++i)
      _columnDists[i] = new int[data.columnClasses(i)][data.classes()];
    // create the columns themselves
    _features = new int[features];
  }

  /** Resets the statistic so that it can be used to compute new node. Creates
   * a new subset of columns that will be analyzed and clears their
   * distribution arrays.
   */
  void reset(Data data) {
    // first get the columns for current split via Resevoir Sampling
    int i = 0;
    for( ; i<_features.length; i++ ) _features[i] = i;
    for( ; i<data.columns() ; i++ ) {
      int off = random.nextInt(i);
      if( off < _features.length ) _features[off] = i;
    }
    // reset the column distributions for those
    for (int j : _features)
      for (int[] d: _columnDists[j])
        Arrays.fill(d,0);
    // and now the statistic is ready
  }

  /** Adds the given row to the statistic. Updates the column distributions for
   * the analyzed columns.
   */
  void add(Row row) {
    for (int i : _features)
      _columnDists[i][row.getColumnClass(i)][row.classOf()] += 1;
  }

  /** Calculates the best split and returns it. The split can be either a split
   * which is a node where all rows with given column value smaller or equal to
   * the split value will go to the left and all greater will go to the right.
   * Or it can be an exclusion split, where all rows with column value equal to
   * split value go to the left and all others go to the right.
   */
  Split split(Data d) {
    // initialize the distribution array
    int[] dist = new int[d.classes()];
    int distWeight = aggregateColumn(_features[0], dist);
    // check if we are leaf node
    int m = Utils.maxIndex(dist, random); //FIXME:take care of the case where there are several classes
    if( dist[m] == distWeight )  return Split.constant(m);

    // try the splits
    Split bestSplit = columnSplit(_features[0],d, dist, distWeight);
    for( int j = 1; j < _features.length; ++j ) {
      Split s = columnSplit(_features[j],d, dist, distWeight);
      if( s.betterThan(bestSplit) )
        bestSplit = s;
    }
    // if we are an impossible split now, we can't get better by the exclusion
    if( bestSplit.isImpossible() )
      return bestSplit;
    assert !bestSplit.isLeafNode(); // Constant leaf splits already tested for above
    
    // try the exclusions now if some of them will be better
    for( int j = 0; j < _features.length; ++j) {
      Split s = columnExclusion(_features[j],d,dist,distWeight);
      if( s.betterThan(bestSplit) )
        bestSplit = s;
    }
    return bestSplit;
  }
}

