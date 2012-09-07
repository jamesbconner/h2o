/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;

/**
 *
 * @author peta
 */
public abstract class BaseStatistic {
  
  /** Returns the best split for a given column
   * 
   * @param colIndex
   * @return 
   */
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
    public static Split constant(int result) {
      return new Split(-1, result, -1);
    }
    
    public static Split impossible(int result) {
      return new Split(-2, result, -1);
    }
    
    public final boolean isLeafNode() {
      return column < 0;
    }
    
    public final boolean isConstant() {
      return column == -1;
    }
    
    public final boolean isImpossible() {
      return column == -2;
    }
    
    public final boolean betterThan(Split other) {
      return fitness > other.fitness;
    }
  }
  /// Column distributions for the given statistic
  protected final double[][][] columnDists_;
  /// Columns that are currently used.
  protected final int[] columns_;
  
  
  
  private final int[] tempCols_;
  
  public BaseStatistic(Data data) {
    // first create the column distributions
    columnDists_ = new double[data.columns()][][];
    for (int i = 0; i < columnDists_.length; ++i)
      columnDists_[i] = new double[data.columnClasses(i)][data.classes()];
    // create the columns themselves
    columns_ = new int[data.features()];
    // create the temporary column array to choose cols from
    tempCols_ = new int[data.columns()];
  }
  
  /** Resets the statistic so that it can be used to compute new node. 
   * 
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
      for (double[] d: columnDists_[j])
        Arrays.fill(d,0.0);
    // and now the statistic is ready
  }
  
  /** Adds the given row to the statistic. 
   * 
   * @param row 
   */
  public void add(Row row) {
    for (int i : columns_)
      columnDists_[i][row.getColumnClass(i)][row.classOf()] += row.weight();
  }
  
  /** Calculates the best split and returns it. 
   * 
   * @return 
   */
  public Split split() {
    Split bestSplit = columnSplit(columns_[0]);
    if (!bestSplit.isConstant())
      for (int j = 1; j < columns_.length; ++j) {
        Split s = columnSplit(columns_[j]);
        if (s.betterThan(bestSplit))
        bestSplit = s;
      }
    return bestSplit;
  }
  
}

