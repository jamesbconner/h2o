package hexlytics.rf;

import java.util.Arrays;

/** This is an entropy statistic calculation. 
 * 
 * @author peta
 */
class EntropyStatistic extends BaseStatistic {
  double[] left;
  double[] right;
  
  public EntropyStatistic(Data data, int features) {
    super(data, features);
    left = new double[data.classes()];
    right = new double[data.classes()];
  }
  
  @Override protected Split columnSplit(int colIndex) {
    // get the weight and left & right distributions, initialize the split info
    Arrays.fill(left,0);
    System.arraycopy(dist_, 0, right, 0, dist_.length);
    double totParent = weight_;
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
      if (totLeft == 0)
        continue;
      for (double e: left)
        if (e != 0)
          eLeft -= (e/totLeft) * Math.log(e/totLeft);
      for (double e: right)
        totRight += e;
      if (totRight == 0)
        continue;
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
  
} 
