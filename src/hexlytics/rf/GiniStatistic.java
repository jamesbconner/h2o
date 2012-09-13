package hexlytics.rf;

import java.util.Arrays;

public class GiniStatistic extends BaseStatistic {
  double[] leftDist;
  double[] rightDist;
  
  
  public GiniStatistic(Data data, int features) { 
    super(data, features);
    leftDist = new double[data.classes()];
    rightDist = new double[data.classes()];
  }
  
  private double gini(double[] dd, double sum) {
    double result = 1;
    for (double d : dd)  result -= (d/sum) * (d/sum);
    return result;
  }
  
  /** Returns the best split for given column. */
  @Override protected Split columnSplit(int colIndex) {
    Arrays.fill(leftDist,0);
    System.arraycopy(dist_, 0, rightDist, 0, dist_.length);
    double leftWeight = 0;
    double rightWeight = weight_;
    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 2;
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
      double f = gini(leftDist,leftWeight) * (leftWeight / weight_) + gini(rightDist,rightWeight) * (rightWeight / weight_);
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
