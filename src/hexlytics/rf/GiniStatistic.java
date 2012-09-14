package hexlytics.rf;

/** Computes the gini split statistics.
 *
 * The Gini fitness is calculated as a probability that the element will be
 * misclassified, which is:
 *
 * 1 - \sum(p_i^2)
 *
 * This is computed for the left and right subtrees and added together:
 *
 * gini left * weight left + gini right * weight left
 * --------------------------------------------------
 *                weight total
 *
 * And subtracted from an ideal worst 1 to simulate the gain from previous node.
 * The best gain is then selected. Same is done for exclusions, where again
 * left stands for the rows with column value equal to the split value and
 * right for all different ones.
 */
public class GiniStatistic extends Statistic {
  private static final double MIN_ERROR_RATE = 0.0;

  public GiniStatistic(Data data, int features) { super(data, features); }

  private double gini(int[] dd, int sum) {
    double result = 1.0;
    for (int d : dd) {
      double tmp = ((double)d)/sum;
      result -= tmp*tmp;
    }
    return result;
  }

  private int _bestSplit;
  private double _bestFitness;

  private Split findSplit(int colIndex, Data d) {
    int[] leftDist = new int[columnDists_[colIndex][0].length];
    int[] rightDist = new int[leftDist.length];
    int leftWeight = 0;
    int rightWeight = aggregateColumn(colIndex, rightDist);

    // check if we are below the error rate proposed and if so,
    //    return the leaf node split.
    // Note that this cuts out single-class collections also.
    int maxIndex = Utils.maxIndex(rightDist);
    if (((double)(rightDist[maxIndex])/rightWeight) >= 1.0-MIN_ERROR_RATE)
      return Split.constant(maxIndex);
    int totWeight = rightWeight;

    // we are not a single class, calculate the best split for the column
    _bestSplit = -1;
    _bestFitness = 2.0;   // Fitness to minimize
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
      if( leftWeight == 0 || rightWeight == 0 ) continue;
      double f =
        gini(leftDist ,leftWeight ) * ((double)leftWeight  / totWeight) +
        gini(rightDist,rightWeight) * ((double)rightWeight / totWeight);
      if( f<_bestFitness ) { // Take split with smallest fitness
        _bestSplit = i;
        _bestFitness = f;
      }
    }
    if( _bestSplit == -1 ) return Split.impossible(Utils.maxIndex(dist_, d.random()));
    return null;
  }

  @Override protected Split columnSplit(int colIndex, Data d) {
    Split split = findSplit(colIndex, d);
    if( split != null ) return split;
    return Split.split(colIndex, _bestSplit, 1.0-_bestFitness);
  }

  @Override protected Split columnExclusion(int colIndex, Data d) {
    Split split = findSplit(colIndex, d);
    if( split != null ) return split;
    return Split.exclusion(colIndex, _bestSplit, _bestFitness);
  }
}
