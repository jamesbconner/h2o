/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import java.util.Arrays;

/**
 *
 * @author peta
 */
public class GiniExclusionStatistic extends BaseStatistic {
  double[] excluded;
  double[] others;

  private double gini(double[] dd, double sum) {
    double result = 1;
    for (double d : dd)  result -= (d/sum) * (d/sum);
    return result;
  }
  
  public GiniExclusionStatistic(Data data, int features) {
    super(data, features);
    excluded = new double[data.classes()];
    others = new double[data.classes()];
  }
  /** Returns the best split for given column. */
  @Override protected Split columnSplit(int colIndex) {
    Arrays.fill(excluded,0);
    System.arraycopy(dist_, 0, others, 0, dist_.length);
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 2;
    for (int i = 0; i < columnDists_[colIndex].length-1; ++i) {
      double excludedWeight = 0;
      double othersWeight = weight_;
      // first copy the i-th guys from right to left
      for (int j = 0; j < excluded.length; ++j) {
        double t = columnDists_[colIndex][i][j];
        excludedWeight += t;
        othersWeight -= t;
        others[j] += excluded[j];
        excluded[j] = t;
        others[j] -= t;
      }
      // now make sure we have something to split 
      if ((excludedWeight == 0) || (othersWeight == 0))
        continue;
      double f = gini(excluded,excludedWeight) * (excludedWeight/weight_) + gini(others,othersWeight) * (othersWeight / weight_);
      if (f<bestFitness) {
        bestSplit = i;
        bestFitness = f;
      }
    }    
    // if we have no split, then get the most common element and return it as
    // a constant split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_));
    else 
      return new Split(colIndex,bestSplit,1-bestFitness);
  }
  
}
