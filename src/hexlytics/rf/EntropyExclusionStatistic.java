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
public class EntropyExclusionStatistic extends Statistic {
  double[] excluded;
  double[] others;
  
  public EntropyExclusionStatistic(Data data, int features) {
    super(data, features);
    excluded = new double[data.classes()];
    others = new double[data.classes()];
  }
  
  @Override protected Split columnSplit(int colIndex, Data d) {
    // get the weight and left & right distributions, initialize the split info
    Arrays.fill(excluded,0);
    System.arraycopy(dist_, 0, others, 0, dist_.length);
    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < columnDists_[colIndex].length; ++i) {
      double eExcluded = 0;
      double totExcluded = 0;
      double totOthers = 0;
      double eOthers = 0;
      for (int j = 0; j < excluded.length; ++j) {
        double v = columnDists_[colIndex][i][j];
        others[j] += excluded[j];
        excluded[j] = v;
        others[j] -= v;
      }
      for (double e: excluded)
        totExcluded += e;
      if (totExcluded == 0)
        continue;
      for (double e: excluded)
        if (e != 0)
          eExcluded -= (e/totExcluded) * Math.log(e/totExcluded);
      for (double e: others)
        totOthers += e;
      if (totOthers == 0)
        continue;
      for (double e: others)
        if (e != 0)
          eOthers -= (e/totOthers) * Math.log(e/totOthers);
      double eReduction = 1 - (eExcluded * totExcluded + eOthers * totOthers) / weight_; 
      if (eReduction > maxReduction) {
        bestSplit = i;
        maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    if (bestSplit == -1)
      return Split.impossible(Utils.maxIndex(dist_,d.random()));
    else 
      return new Split(colIndex,bestSplit,maxReduction);
  } 
  
}
