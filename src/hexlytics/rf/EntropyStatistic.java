package hexlytics.rf;

import java.util.Arrays;

/** This is an entropy statistic calculation. 
 * 
 * @author peta
 */
class EntropyStatistic extends BaseStatistic {
  double[][] dists_;
  double weight_;
  
  public EntropyStatistic(Data data) {
    super(data);
    dists_ = new double[2][data.classes()];
  }
 
   public static double entropyOverColumns(double[][] m) {
    double result = 0;
    double total = 0;
    for (int col = 0; col < m[0].length; ++col) {
      double sum = 0;
      for (int row = 0; row < m.length; ++row)
        sum += m[row][col];
      result -= Utils.lnF(sum);
      total += sum;
    }
    return (total == 0) ? 0 : (result + Utils.lnF(total)) / (total * Math.log(2));
  }
  
  public static double entropyCondOverRows(double[][] m) {
    double result = 0;
    double total = 0;
    for (double[] d : m) {
      double sum = 0;
      for (double dd : d) {
        sum += dd;
        result += Utils.lnF(dd);
      }
      result -= Utils.lnF(sum);
      total += sum;
    }
    return (total == 0) ? 0 : -result / (total *Math.log(2));
  }
  
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
  }
}


