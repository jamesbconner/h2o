package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;


/** A better working gini statistic that should be faster. Hopefully much faster. 
 *
 * @author peta
 */
public class GiniStatistic extends BaseStatistic {

  // PETA TODO This should not be here!!!!
  public static final double MIN_ERROR_RATE = 0.0;
  
  
  public GiniStatistic(Data data) {
    super(data);
  }
  
  private double gini(double[] dd, double sum) {
    double result = 1;
    for (double d : dd)
      result -= (d/sum) * (d/sum);
    return result;
  }
  
  /** Aggregates the given column's distribution to the provided array and 
   * returns the sum of weights of that array. 
   * 
   */
  private final double aggregateColumn(int colIndex, double[] dist) {
    double sum = 0;
    for (int j = 0; j < columnDists_[colIndex].length; ++j) {
      for (int i = 0; i < dist.length; ++i) {
        sum += columnDists_[colIndex][j][i];
        dist[i] += columnDists_[colIndex][j][i]; 
      }
    }
    return sum;
  }
  
  private final int singleClass(double[] dist) {
    int result = -1;
    for (int i = 0; i < dist.length; ++i)
      if (dist[i] != 0)
        if (result == dist[i]) {
          // pass
        } else if (result == -1) {
          result = i;
        } else {
          result = -1;
          break;
        }
    return result;
  }

// PETA TODO not used atm
/*  private Split columnExclusion(int colIndex) {
    double[] excluded = new double[columnDists_[colIndex][0].length];
    double[] others = new double[excluded.length];
    double excludedWeight = 0;
    double othersWeight = aggregateColumn(colIndex,others);
    double totWeight = othersWeight;
    // check if we are single class
    int sc = singleClass(others);
    if (sc != -1)
      return Split.constant(sc);
    int bestExcluded = -1;
    double bestFitness = -1;
    for (int i = 0; i < columnDists_[colIndex].length-1; ++i) {
      // first get the i-th column out of the others and put the last excluded back
      othersWeight += excludedWeight;
      excludedWeight = 0;
      for (int j = 0; j < others.length; ++j) {
        double t = columnDists_[colIndex][i][j];
        others[j] += excluded[j] - t;
        excluded[j] = t;
        excludedWeight += t;
      }
      othersWeight -= excludedWeight;
      if (excludedWeight == 0)
        continue;
      if (othersWeight == 0)
        return Split.impossible(Utils.maxIndex(excluded));
      double f = gini(excluded,excludedWeight) * (excludedWeight / totWeight) + gini(others,othersWeight) * (othersWeight / totWeight);
      if (f>bestFitness) {
        bestExcluded = i;
        bestFitness = f;
      }
    }
    assert (bestExcluded != -1);
    return new Split(colIndex, bestExcluded, bestFitness);
  } */
  
  /** Returns the best split for given column. 
   * 
   * @param colIndex
   * @return 
   */
  @Override protected Split columnSplit(int colIndex) {
    double[] leftDist = new double[columnDists_[colIndex][0].length];
    double[] rightDist = new double[leftDist.length];
    double leftWeight = 0;
    double rightWeight = aggregateColumn(colIndex, rightDist);
    // check if we are below the error rate proposed and if so, return the leafnode split instead
    int maxIndex = Utils.maxIndex(rightDist);
    if ((rightDist[maxIndex]/rightWeight) >= 1-MIN_ERROR_RATE)
      return Split.constant(maxIndex);
    double totWeight = rightWeight;
    // now check if we have only a single class
    int singleClass = singleClass(rightDist);
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
      double f = gini(leftDist,leftWeight) * (leftWeight / totWeight) + gini(rightDist,rightWeight) * (rightWeight / totWeight);
      if (f>bestFitness) {
        bestSplit = i;
        bestFitness = f;
      }
    }    
    // if we have no split, then get the most common element and return it as
    // a constant split
    if (bestSplit == -1) {
      // put everything to the left guy
      return Split.impossible(maxIndex);
/*      for (int j = 0; j < leftDist.length; ++j) {
        double t = columnDists_[colIndex][columnDists_[colIndex].length-1][j];
        leftWeight += t;
        leftDist[j] += t;
      }
      int best = 0;
      for (int i = 1; i < leftDist.length; ++i) 
        if (leftDist[i] > leftDist[best])
          best = i;
      return Split.impossible(best); */
    }
    //System.exit(-1);
    return new Split(colIndex,bestSplit,bestFitness);
  }
  
}
