/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;
import sun.security.util.Length;

/** A better working gini statistic that should be faster. Hopefully much faster. 
 *
 * @author peta
 */
public class Gini2 {
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
    
    public final boolean betterThan(Split other) {
      return fitness > other.fitness;
    }
  }
  
  
  /// Column distributions for the given statistic
  private final double[][][] columnDists_;
  /// Columns that are currently used.
  private final int[] columns_;
  
  
  
  private final int[] tempCols_;
  
  public Gini2(Data data) {
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
  
  
  private double gini(double[] dd, double sum) {
    double result = 1;
    for (double d : dd)
      result -= (d/sum) * (d/sum);
    return result;
  }
  
  /** Returns the best split for given column. 
   * 
   * @param colIndex
   * @return 
   */
  private Split columnSplit(int colIndex) {
    double[] leftDist = new double[columnDists_[colIndex][0].length];
    double[] rightDist = new double[leftDist.length];
    double leftWeight = 0;
    double rightWeight = 0;
    for (int j = 0; j < columnDists_[colIndex].length; ++j) {
      for (int i = 0; i < rightDist.length; ++i) {
        rightWeight += columnDists_[colIndex][j][i];
        rightDist[i] = columnDists_[colIndex][j][i]; 
      }
    }
    double totWeight = rightWeight;
    // now check if we have only a single class
    int singleClass = -1;
    for (int i = 0; i < rightDist.length; ++i)
      if (rightDist[i] != 0)
        if (singleClass == -1) {
          singleClass = i;
        } else {
          singleClass = -1;
          break;
        }
    if (singleClass != -1) 
      return Split.constant(singleClass);
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = -1;
    for (int i = 0; i < columnDists_[colIndex].length-1; ++i) {
      // first copy the i-th guys from right to left
      for (int j = 0; j < leftDist.length; ++j) {
        double t = columnDists_[colIndex][i][j];
        leftWeight += t;
        rightWeight -= t;
        leftDist[j] += t;
        rightDist[j] -= t;
      }
      // now make sure we have something to split 
      if ((leftWeight == 0) || (rightWeight == 0))
        continue;
      double f = gini(leftDist,leftWeight) / totWeight + gini(rightDist,rightWeight) / totWeight;
      if (f>bestFitness) {
        bestSplit = i;
        bestFitness = f;
      }
    }    
    // if we have no split, then get the most common element and return it as
    // a constant split
    if (bestSplit == -1) {
      // put everything to the left guy
      for (int j = 0; j < leftDist.length; ++j) {
        double t = columnDists_[colIndex][columnDists_[colIndex].length-1][j];
        leftWeight += t;
        leftDist[j] += t;
      }
      int best = 0;
      for (int i = 1; i < leftDist.length; ++i) 
        if (leftDist[i] > leftDist[best])
          best = i;
      return Split.impossible(best);
    }
    return new Split(colIndex,bestSplit,bestFitness);
  }
  
  /** Calculates the best split and returns it. 
   * 
   * @return 
   */
  public Split split() {
    Split bestSplit = columnSplit(columns_[0]);
    if (bestSplit.isConstant())
      return bestSplit;
    for (int j = 1; j < columns_.length; ++j) {
      Split s = columnSplit(columns_[j]);
      if (s.betterThan(bestSplit))
        bestSplit = s;
    }
    return bestSplit;
  }
  
  
  
  
  
  
  
}