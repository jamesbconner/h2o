/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;

/** A general statistic framework. Keeps track of the column distributions and 
 * analyzes the column splits in the end producing the single split that will
 * be used for the node. 
 * 
 * @author peta
 */
public abstract class Statistic {
  
  // distribution of the row classes
  int [] dist_;
  // total weight of all rows processed by the statistic
  int weight_;
  
  /** Returns the best split for a given column   */
  protected abstract Split columnSplit(int colIndex, Data d);
  protected abstract Split columnExclusion(int colIndex, Data d);
  
  /** Split descriptor for a particular column. 
   * 
   * Holds the column name and the split point, which is the last column class
   * that will go to the left tree. If the column index is -1 then the split
   * value indicates the return value of the node.
   */
  public static class Split {
    public final int column;
    public final int split;
    public final double fitness;
    protected Split(int column, int split, double fitness) {
      this.column = column;
      this.split = split;
      this.fitness = fitness;
    }
    
    /** A constant split used for true leaf nodes where all rows are of the same class.
     */
    public static Split constant(int result) {  return new Split(-1, result, -1); }
    
    /** An impossible split, which behaves like a constant split. However impossible split
     * occurs when there are different row classes present, but they all have
     * the same column value and therefore no split can be made. 
     */
    public static Split impossible(int result) { return new Split(-2, result, -1);  }  
    
    /** Classic split. All lower or equal than split value goes to left, all
     * greater goes to right. 
     */ 
    public static Split split(int column, int split, double fitness) { return new Split(column, split,fitness); }
    
    /** Exclusion split. All equal to split value goes to left, all different
     * goes to right. 
     */
    public static Split exclusion(int column, int split, double fitness) { return new ExclusionSplit(column,split,fitness); }

    public final boolean isLeafNode() { return column < 0; }    
    public final boolean isConstant() { return column == -1; }    
    public final boolean isImpossible() { return column == -2;  } 
    public final boolean betterThan(Split other) { return fitness > other.fitness; }
    public final boolean isExclusion() { return this instanceof ExclusionSplit; }
  }

  /** An exclusion split. All equal to split value go to left node, all others
   * go to the right one. 
   */
  public static class ExclusionSplit extends Split {
    protected ExclusionSplit(int column, int split, double fitness) {
      super(column, split,fitness);
    }
  }

  protected final int[][][] columnDists_;  /// Column distributions for the given statistic
  protected final int[] columns_;// Columns that are currently used.
  
  /** Aggregates the given column's distribution to the provided array and 
   * returns the sum of weights of that array.  */
  protected final int aggregateColumn(int colIndex, int[] dist) {
    int sum = 0;
    for (int j = 0; j < columnDists_[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        sum += columnDists_[colIndex][j][i];
        dist[i] += columnDists_[colIndex][j][i]; 
      }
    }
    return sum;
  }
  
  // field used to calculate the analyzed columns
  private final int[] tempCols_;
  // number of features of the dataset associated with the statistic. 
  private final int _features;
  
  public Statistic(Data data, int features) {
    _features = features;
    // first create the column distributions
    columnDists_ = new int[data.columns()][][];
    for (int i = 0; i < columnDists_.length; ++i)
      columnDists_[i] = new int[data.columnClasses(i)][data.classes()];
    // create the columns themselves
    columns_ = new int[_features];
    // create the temporary column array to choose cols from
    tempCols_ = new int[data.columns()];
    dist_ = new int[data.classes()];
    weight_ = 0;
  }
  
  /** Resets the statistic so that it can be used to compute new node. Creates
   * a new subset of columns that will be analyzed and clears their
   * dirstribution arrays. 
   */
  public void reset(Data data) {
    // first get the columns for current split
    Arrays.fill(tempCols_,0);
    int i = 0;
    while (i < columns_.length) {
      int off = data.random().nextInt(tempCols_.length);
      if (tempCols_[off] == -1)
        continue;
      tempCols_[off] = -1;
      columns_[i] = off;
      ++i;
    }
    // reset the column distributions for those
    for (int j : columns_) 
      for (int[] d: columnDists_[j])
        Arrays.fill(d,0);
    // and now the statistic is ready
  }
  
  /** Adds the given row to the statistic. Updates the column distributions for
   * the analyzed columns. 
   */
  public void add(Row row) {
    for (int i : columns_)
      columnDists_[i][row.getColumnClass(i)][row.classOf()] += 1; 
  }
  
  /** Calculates the best split and returns it. The split can be either a split
   * which is a node where all rows with given column value smaller or equal to
   * the split value will go to the left and all greater will go to the right. 
   * Or it can be an exclusion split, where all rows with column value equal to
   * split value go to the left and all others go to the right. 
   */
  public Split split(Data d) {
    // initialize the distribution array
    Arrays.fill(dist_,0);
    weight_ = aggregateColumn(columns_[0], dist_);
    // check if we are leaf node
    int m = Utils.maxIndex(dist_, d.random());
    if ( dist_[m] == weight_)
      return Split.constant(m);
    // try the splits
    Split bestSplit = columnSplit(columns_[0],d);
    for (int j = 1; j < columns_.length; ++j) {
      Split s = columnSplit(columns_[j],d);
      if (s.betterThan(bestSplit))
        bestSplit = s;
    }
    // if we are a leaf node now, we can't get better by the exclusion 
    if (bestSplit.isLeafNode())
      return bestSplit;
    // try the exclusions now if some of them will be better
    for (int j = 0; j < columns_.length; ++j) {
      Split s = columnExclusion(columns_[j],d);
      if (s.betterThan(bestSplit))
        bestSplit = s;
    }
    return bestSplit;
  }
}

