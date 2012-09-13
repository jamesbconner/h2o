package hexlytics.rf;

import java.util.Arrays;

/** This is an entropy statistic calculation. 
 * 
 * @author peta
 */
class EntropyStatistic extends BaseStatistic {
  double[][] dists_;
  
  public EntropyStatistic(Data data, int features) {
    super(data, features);
    dists_ = new double[2][data.classes()];
  }
  
/*  private double entropyOverDist(double[] dist, double weight) {
    double result = 0;
    for (double d: dist)
      if (d!=0) 
        result -= (d/weight) * Math.log(d/weight);
    return result;
  } */
  
/*  private void resetDists(int colIndex) {
    if (columns_[0] == colIndex) {
      Arrays.fill(dists_[0],0);
      Arrays.fill(dists_[1],0);
      weight_ = aggregateColumn(colIndex, dists_[1]);
    } else {
      double[] d = dists_[0];
      dists_[0] = dists_[1];
      dists_[1] = d;
    }
  } */
  
  @Override protected Split columnSplit(int colIndex) {
    // get the weight and left & right distributions, initialize the split info
    Arrays.fill(dists_[0],0);
    System.arraycopy(dist_, 0, dists_[1], 0, dist_.length);
    double totParent = weight_;
    double[] left = dists_[0];
    double[] right = dists_[1];
    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      double eLeft = 0;
      double totLeft = 0;
      double totRight = 0;
      double eRight = 0;
      for (int j = 0; j < left.length; ++j) {
        double v = columnDists_[colIndex][i][j];
        left[j] += v;
        right[j] -= v;
      }
      for (double e: left)
        totLeft += e;
      for (double e: left)
        if (e != 0)
          eLeft -= (e/totLeft) * Math.log(e/totLeft);
      for (double e: right)
        totRight += e;
      for (double e: right)
        if (e != 0)
          eRight -= (e/totRight) * Math.log(e/totRight);
      double eReduction = 1 - (eLeft * totLeft + eRight * totRight) / totParent; 
      if (eReduction > maxReduction) {
        bestSplit = i;
        maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_));
    else 
      return new Split(colIndex,bestSplit,maxReduction);
  } 
  
/*  @Override protected Split columnSplit(int colIndex) {
    // reset or reuse the dists
    resetDists(colIndex);
    // check if using leaf node fits into the specified MIN_ERROR_RATE
    int maxIndex = Utils.maxIndex(dists_[1]);
    if (dists_[1][maxIndex]/weight_ >= 1-MIN_ERROR_RATE)
      return BaseStatistic.Split.constant(maxIndex);
    // calculate the best split
    double bestF = -1;
    int bestSplit = -1;
    double lw = 0;
    double rw = weight_;
//    System.out.println("column "+colIndex);
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      // move the column from right to left
      double[] cd = columnDists_[colIndex][i];
      for (int j = 0; j < cd.length; ++j) {
        dists_[0][j] += cd[j];
        dists_[1][j] -= cd[j];
        lw += cd[j];
        rw -= cd[j];
      }
      if ((lw==0) || (rw==0))
        continue;
      double f = 1 - (entropyOverDist(dists_[0],lw)*lw + entropyOverDist(dists_[1],rw)*rw) / weight_;
//      System.out.println("  "+i+": "+f);
      if (f > bestF) {
        bestSplit = i;
        bestF = f;
      }
    }
    // check if we have the proper split 
    if (bestSplit == -1)
      return BaseStatistic.Split.impossible(maxIndex);
    else
      return new BaseStatistic.Split(colIndex,bestSplit,bestF);
  } */ 
  
  /*
  @Override protected BaseStatistic.Split columnSplit(int colIndex) {
    if (columns_[0] == colIndex) {
      Arrays.fill(dists_[0],0);
      Arrays.fill(dists_[1],0);
      weight_ = aggregateColumn(colIndex, dists_[1]);
    } else {
      double[] d = dists_[0];
      dists_[0] = dists_[1];
      dists_[1] = d;
    }
    int maxIndex = Utils.maxIndex(dists_[1]);
    if (dists_[1][maxIndex]/weight_ >= 1-MIN_ERROR_RATE)
      return BaseStatistic.Split.constant(maxIndex);
    // single class is redundant
    double fit = entropyOverColumns(dists_);
    double bestFit = -Double.MAX_VALUE;
    double gain = 0;
    int split = -1;
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      double[] cd = columnDists_[colIndex][i];
      for (int j = 0; j < cd.length; ++j) {
        dists_[0][j] += cd[j];
        dists_[1][j] -= cd[j];
      }
      gain = entropyCondOverRows(dists_);
      double newFit = fit - gain; // fitness gain
      if (newFit == 0)
        continue;
      if (newFit > bestFit) {
        bestFit = newFit;
        split = i;
      }
    }
    if (split == -1)
      return BaseStatistic.Split.impossible(maxIndex);
    else
      return new BaseStatistic.Split(colIndex,split,bestFit);
  } */
} 


