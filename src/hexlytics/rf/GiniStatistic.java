package hexlytics.rf;

public class GiniStatistic extends BaseStatistic {

  public GiniStatistic(Data data, int features) { super(data, features); }

  private double gini(int[] dd, int sum) {
    double result = 1.0;
    for (int d : dd) {
      double tmp = ((double)d)/sum;
      result -= tmp*tmp;
    }
    return result;
  }

  /** Returns the best split for given column. */
  @Override protected Split columnSplit(int colIndex) {
    int[] leftDist = new int[columnDists_[colIndex][0].length];
    int[] rightDist = new int[leftDist.length];
    int leftWeight = 0;
    int rightWeight = aggregateColumn(colIndex, rightDist);
    // check if we are below the error rate proposed and if so, return the leafnode split instead
    int maxIndex = Utils.maxIndex(rightDist);
    if (((double)(rightDist[maxIndex])/rightWeight) >= 1.0-MIN_ERROR_RATE)
      return Split.constant(maxIndex);
    int totWeight = rightWeight;

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
      double f =
        gini(leftDist ,leftWeight ) * ((double)leftWeight  / totWeight) +
        gini(rightDist,rightWeight) * ((double)rightWeight / totWeight);
      if (f<bestFitness) {
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
    //System.out.println(colIndex + " - " + bestSplit);
    return new Split(colIndex,bestSplit,1-bestFitness);
  }

}
