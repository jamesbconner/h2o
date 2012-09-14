/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;

public abstract class BaseStatistic {

  // PETA TODO This should not be here!!!!
  public static final double MIN_ERROR_RATE = 0.0;

  /** Returns the best split for a given column   */
  protected abstract Split columnSplit(int colIndex);

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
    public Split(int column, int split, double fitness) {
      this.column = column;
      this.split = split;
      this.fitness = fitness;
    }
    public static Split constant(int result) {  return new Split(-1, result, -1); }
    public static Split impossible(int result) { return new Split(-2, result, -1);  }
    public final boolean isLeafNode() { return column < 0; }
    public final boolean isConstant() { return column == -1; }
    public final boolean isImpossible() { return column == -2;  }
    public final boolean betterThan(Split other) { return fitness > other.fitness; }
  }

  /** Aggregates the given column's distribution to the provided array and
   * returns the sum of weights of that array.  */
  protected final int aggregateColumn(int colIndex, int[] dist) {
    int sum = 0;
    for (int j = 0; j < columnDists_[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        int tmp = columnDists_[colIndex][j][i];
        sum     += tmp;
        dist[i] += tmp;
      }
    }
    return sum;
  }

  protected void showColumnDist(int colIndex) {
    for (int[] d : columnDists_[colIndex])
        System.out.print(" "+Utils.sum(d));
  }

  protected final int[][][] columnDists_;  // Column distributions for the given statistic
  protected final int[] _features;// Columns that are currently used.
  private final int _num_columns;

  public BaseStatistic(Data data, int features) {
    _num_columns = data.columns();
    // first create the column distributions
    columnDists_ = new int[_num_columns][][];
    for (int i = 0; i < columnDists_.length; ++i)
      columnDists_[i] = new int[data.columnClasses(i)][data.classes()];
    // create the columns themselves
    _features = new int[features];
  }

  /** Resets the statistic so that it can be used to compute new node.
   */
  public void reset(Data data) {
    int i = 0;
    for( ; i < _features.length; ++i ) _features[i] = i;
    for( ; i < _num_columns; ++i ) {
      int off = data.random().nextInt(i);
      if( off < _features.length ) _features[off] = i;
    }

    // reset the column distributions for those
    for (int j : _features)
      for (int[] d: columnDists_[j])
        Arrays.fill(d,0);
    // and now the statistic is ready
  }

  /** Adds the given row to the statistic.    */
  public void add(Row row) {
    for (int i : _features)
      columnDists_[i][row.getColumnClass(i)][row.classOf()]++;
  }

  /** Calculates the best split and returns it.  */
  public Split split() {
    Split bestSplit = columnSplit(_features[0]);
    if (!bestSplit.isConstant())
      for (int j = 1; j < _features.length; ++j) {
        Split s = columnSplit(_features[j]);
        if (s.betterThan(bestSplit))
        bestSplit = s;
      }
    return bestSplit;
  }
}

