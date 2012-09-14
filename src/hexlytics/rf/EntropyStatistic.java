package hexlytics.rf;

import java.util.Arrays;

/** This is an entropy statistic calculation. 
 * 
 * The entropy formula is the classic Shannon entropy gain, which is:
 * 
 * - \sum(p_i * log2(_pi))
 * 
 * where p_i is the probability of i-th class occurring. The entropy is
 * calculated for the left and right node after the given split and they are
 * combined together weighted on their probability. 
 * 
 * ent left * weight left + ent right * weight right 
 * --------------------------------------------------
 *                  total weight
 * 
 * And to get the gain, this is subtracted from potential maximum of 1
 * simulating the previous node. The biggest gain is selected as the tree split.
 * 
 * The same is calculated also for exclusion, where left stands for the rows
 * where column equals to the split point and right stands for all others. 
 * 
 * @author peta
 */
class EntropyStatistic extends Statistic {
  int[] left;
  int[] right;
  
  public EntropyStatistic(Data data, int features) {
    super(data, features);
    left = new int[data.classes()];
    right = new int[data.classes()];
  }
  
  @Override protected Split columnSplit(int colIndex, Data d) {
    // get the weight and left & right distributions, initialize the split info
    Arrays.fill(left,0);
    System.arraycopy(dist_, 0, right, 0, dist_.length);
    int totParent = weight_;
    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      double eLeft = 0;
      int totLeft = 0;
      int totRight = 0;
      double eRight = 0;
      for (int j = 0; j < left.length; ++j) {
        double v = columnDists_[colIndex][i][j];
        left[j] += v;
        right[j] -= v;
      }
      for (int e: left)
        totLeft += e;
      if (totLeft == 0)
        continue;
      for (int e: left)
        if (e != 0)
          eLeft -= (e/(double)totLeft) * Math.log(e/(double)totLeft);
      for (int e: right)
        totRight += e;
      if (totRight == 0)
        continue;
      for (int e: right)
        if (e != 0)
          eRight -= (e/(double)totRight) * Math.log(e/(double)totRight);
      double eReduction = 1 - (eLeft * totLeft + eRight * totRight) / (double)totParent; 
      if (eReduction > maxReduction) {
        bestSplit = i;
        maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_,d.random()));
    else 
      return Split.split(colIndex,bestSplit,maxReduction);
  }
  
  @Override protected Split columnExclusion(int colIndex, Data d) {
    // get the weight and left & right distributions, initialize the split info
    Arrays.fill(left,0);
    System.arraycopy(dist_, 0, right, 0, dist_.length);
    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      double eLeft = 0;
      int totLeft = 0;
      int totRight = 0;
      double eRight = 0;
      for (int j = 0; j < left.length; ++j) {
        int v = columnDists_[colIndex][i][j];
        right[j] += left[j];
        left[j] = v;
        right[j] -= v;
      }
      for (int e: left)
        totLeft += e;
      if (totLeft == 0)
        continue;
      for (int e: left)
        if (e != 0)
          eLeft -= (e/(double)totLeft) * Math.log(e/(double)totLeft);
      for (int e: right)
        totRight += e;
      if (totRight == 0)
        continue;
      for (double e: right)
        if (e != 0)
          eRight -= (e/(double)totRight) * Math.log(e/totRight);
      double eReduction = 1 - (eLeft * totLeft + eRight * totRight) / (double)weight_; 
      if (eReduction > maxReduction) {
        bestSplit = i;
        maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_,d.random()));
    else
      return Split.exclusion(colIndex,bestSplit,maxReduction);
  } 
  
} 
