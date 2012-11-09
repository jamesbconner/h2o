package hex.rf;

import hex.rf.Data.Row;

import java.util.*;

/** A general statistic framework. Keeps track of the column distributions and
 * analyzes the column splits in the end producing the single split that will
 * be used for the node.
 */
abstract class Statistic {
  /** Column distributions:  column index x column arity x classes
   *  Remembers the number of rows of the given column index, encodedValue, class.  */
  protected final int[][][] _columnDists;
  protected final int[] _features;         // Columns/features that are currently used.
  protected Random _random;                // Pseudo random number generator
  private int _seed;                       // Seed for prng
  private HashSet<Integer> _remembered;    // Features already used
  final double[] _classWt;                 // Class weights

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
    _random = new Random(seed);
    // first create the column distributions
    _columnDists = new int[data.columns()][][];
    for (int i = 0; i < _columnDists.length; ++i)
      _columnDists[i] = data.ignore(i) ? null : new int[data.columnArity(i)+1][data.classes()];
    // create the columns themselves
    _features = new int[features];
    _remembered = null;
    _classWt = data.classWt();  // Class weights
  }

  // Remember a set of features that were useless in splitting this set of rows
  // - so we can grab new random features and avoid these useless ones.
  // Returns false if all features have been grabbed and we simple cannot
  // distinguish this set of rows.
  boolean remember_features(Data data) {
    // Check that we have enough properties left
    if( _remembered == null ) _remembered = new HashSet<Integer>();
    int used = _remembered.size() + _features.length;
    int available = -1; // we don't count the class column
    for(int i=0;i<data.columns();i++) if(!data.ignore(i)) available++;
    if( used+ _features.length >= available ) return false; // we have tried all the features.
    for(int i=0;i<_features.length;i++) _remembered.add(_features[i]);
    return true;
  }
  void forget_features() { _remembered = null; }

  private boolean isColumnUsable(Data d, int i) {
    assert i < d.columns() : ""+i+"<"+d.columns();
    return d.classIdx() != i
        && !d.ignore(i)
        && (_remembered == null || !_remembered.contains(i));
  }

  /** Resets the statistic so that it can be used to compute new node. Creates
   * a new subset of columns that will be analyzed and clears their
   * distribution arrays.   */
  void reset(Data data, int seed) {
    _random = new Random(_seed = seed);
    // first get the columns for current split via Reservoir Sampling
    // http://en.wikipedia.org/wiki/Reservoir_sampling
    // Pick from all the columns-1, and if we chose the class column,
    // replace it with the last column.
    // Columns that have been marked as ignore should not be selected.
    int i = 0, j = 0;
    for( ; j<_features.length; i++) if (isColumnUsable(data, i)) _features[j++] = i;
    for( ; i<data.columns(); i++ ) {
      if( !isColumnUsable(data, i) ) continue;
      int off = _random.nextInt(i);
      if( off < _features.length ) _features[off] = i;
    }
    for( int k : _features) assert !data.ignore(k);
    for( int k : _features) assert k != data.classIdx();
    // reset the column distributions for those
    for( int k : _features) for( int[] d: _columnDists[k]) Arrays.fill(d,0);
  }

  /** Adds the given row to the statistic. Updates the column distributions for
   * the analyzed columns.
   */
  void addQ(Row row) {
    final int cls = row.classOf();
    for (int i : _features)
      _columnDists[i][row.getEncodedColumnValue(i)][cls]++;
  }

  // Apply any class weights to the distributions.
  void applyClassWeights() {
    if( _classWt == null ) return;
    for( int f : _features ) // For all columns, get the distribution
      for( int[] clss : _columnDists[f] ) // For all distributions, get the class distribution
        for( int cls=0; cls<clss.length; cls++ )
          clss[cls] = (int)(clss[cls]*_classWt[cls]); // Scale by the class weights
  }

  /** Calculates the best split and returns it. The split can be either a split
   * which is a node where all rows with given column value smaller or equal to
   * the split value will go to the left and all greater will go to the right.
   * Or it can be an exclusion split, where all rows with column value equal to
   * split value go to the left and all others go to the right.
   */
  Split split(Data d,boolean expectLeaf) {
    // initialize the distribution array
    int[] dist = new int[d.classes()];
    int distWeight = aggregateColumn(_features[0], dist);
    // check if we are leaf node
    int m = Utils.maxIndex(dist, _random); //FIXME:take care of the case where there are several classes
    if( expectLeaf || (dist[m] == distWeight ))  return Split.constant(m);
    // try the splits
    Split bestSplit = Split.split(_features[0], 0, -Double.MAX_VALUE);
    for( int j = 0; j < _features.length; ++j ) {
      Split s = columnSplit(_features[j],d, dist, distWeight);
      if( s.betterThan(bestSplit) )
        bestSplit = s;
    }
    // if we are an impossible split now, we can't get better by the exclusion
    if( bestSplit.isImpossible() ) {
      // See if we have enough features to try again with all new features.
      if( !remember_features(d) ) return bestSplit;
      reset(d,_seed+1);         // Reset with new features
      for(Row r: d)  addQ(r);   // Reload the distributions
      applyClassWeights();      // Weight the distributions
      bestSplit = split(d,expectLeaf);
      if (bestSplit.isImpossible()) return bestSplit;
    }
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

