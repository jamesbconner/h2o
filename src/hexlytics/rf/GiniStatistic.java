package hexlytics.rf;

import java.util.Arrays;

public class GiniStatistic extends Statistic {
  int[] leftDist;
  int[] rightDist;
  
  
  public GiniStatistic(Data data, int features) { 
    super(data, features);
    leftDist = new int[data.classes()];
    rightDist = new int[data.classes()];
  }
  
  private double gini(int[] dd, double sum) {
    double result = 1;
    for (int d : dd)  result -= (d/sum) * (d/sum);
    return result;
  }
  
  /** Returns the best split for given column. */
  @Override protected Split columnSplit(int colIndex,Data d) {
    Arrays.fill(leftDist,0);
    System.arraycopy(dist_, 0, rightDist, 0, dist_.length);
    int leftWeight = 0;
    int rightWeight = weight_;
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 2;
    for (int i = 0; i < columnDists_[colIndex].length-1; ++i) {
      // first copy the i-th guys from right to left
      for (int j = 0; j < leftDist.length; ++j) {
        int t = columnDists_[colIndex][i][j];
        leftWeight += t;
        rightWeight -= t;
        leftDist[j] += t;
        rightDist[j] -= t;
      }
      // now make sure we have something to split 
      if ((leftWeight == 0) || (rightWeight == 0))
        continue;
      double f = gini(leftDist,leftWeight) * (leftWeight / (double)weight_) + gini(rightDist,rightWeight) * (rightWeight / (double)weight_);
      if (f<bestFitness) {
        bestSplit = i;
        bestFitness = f;
      }
    }    
    // if we have no split, then get the most common element and return it as
    // a constant split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_,d.random()));
    else 
      return Split.split(colIndex,bestSplit,1-bestFitness);
  }
  
  /** Returns the best split for given column. */
  @Override protected Split columnExclusion(int colIndex, Data d) {
    return null;
//    Arrays.fill(excluded,0);
//    System.arraycopy(dist_, 0, others, 0, dist_.length);
//    // we are not a single class, calculate the best split for the column
//    int bestSplit = -1;
//    double bestFitness = 2;
//    for (int i = 0; i < columnDists_[colIndex].length-1; ++i) {
//      double excludedWeight = 0;
//      double othersWeight = weight_;
//      // first copy the i-th guys from right to left
//      for (int j = 0; j < excluded.length; ++j) {
//        double t = columnDists_[colIndex][i][j];
//        excludedWeight += t;
//        othersWeight -= t;
//        others[j] += excluded[j];
//        excluded[j] = t;
//        others[j] -= t;
//      }
//      // now make sure we have something to split 
//      if ((excludedWeight == 0) || (othersWeight == 0))
//        continue;
//      double f = gini(excluded,excludedWeight) * (excludedWeight/weight_) + gini(others,othersWeight) * (othersWeight / weight_);
//      if (f<bestFitness) {
//        bestSplit = i;
//        bestFitness = f;
//      }
//    }    
//    // if we have no split, then get the most common element and return it as
//    // a constant split
//    if (bestSplit == -1)
//      return Split.impossible(Utils.maxIndex(dist_));
//    else 
//      return new Split(colIndex,bestSplit,1-bestFitness);
  }
  
}
