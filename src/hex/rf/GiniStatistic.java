package hex.rf;

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

  public GiniStatistic(Data data, int features, int seed) { super(data, features, seed); }

  private double gini(int[] dd, int sum) {
    double result = 1.0;
    for (int d : dd) {
      double tmp = ((double)d)/sum;
      result -= tmp*tmp;
    }
    return result;
  }

  @Override protected Split columnSplit(int colIndex, Data d, int[] dist, int distWeight) {
    int[] leftDist = new int[d.classes()];
    int[] riteDist = dist.clone();
    int leftWeight = 0;
    int riteWeight = distWeight;
    int totWeight = riteWeight;

    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 0.0;   // Fitness to maximize
    assert !d.ignore(colIndex);
    //assert _columnDists[colIndex].length > 1; // Have 2 have at least 2 rows to split
    //assert leftDist.length==_columnDists[colIndex][0].length;

    for (int i = 0; i < _columnDists[colIndex].length-1; ++i) {
      // first copy the i-th guys from rite to left
      for (int j = 0; j < leftDist.length; ++j) {
        int t = _columnDists[colIndex][i][j];
        leftWeight += t;
        riteWeight -= t;
        leftDist[j] += t;
        riteDist[j] -= t;
      }
      // now make sure we have something to split
      if( leftWeight == 0 || riteWeight == 0 ) continue;
      double f = 1.0 -
        (gini(leftDist,leftWeight) * ((double)leftWeight / totWeight) +
         gini(riteDist,riteWeight) * ((double)riteWeight / totWeight));
      if( f>bestFitness ) { // Take split with largest fitness
        bestSplit = i;
        bestFitness = f;
      }
    }
    return bestSplit == -1
      ? Split.impossible(Utils.maxIndex(dist, random))
      : Split.split(colIndex, bestSplit, bestFitness);
  }

  @Override protected Split columnExclusion(int colIndex, Data d, int[] dist, int distWeight) {
    int[] inclDist = new int[d.classes()];
    int[] exclDist = dist.clone();

    // we are not a single class, calculate the best split for the column
    int bestSplit = -1;
    double bestFitness = 0.0;   // Fitness to maximize
    for( int i = 0; i < _columnDists[colIndex].length-1; ++i ) {
      // first copy the i-th guys from rite to left
      int sumt = 0;
      for( int j = 0; j < inclDist.length; ++j ) {
        int t = _columnDists[colIndex][i][j];
        sumt += t;
        inclDist[j] = t;
        exclDist[j] = dist[j] - t;
      }
      int inclW = sumt;
      int exclW = distWeight - inclW;
      // now make sure we have something to split
      if( inclW == 0 || exclW == 0 ) continue;
      double f = 1.0 -
        (gini(inclDist,inclW) * ((double)inclW / distWeight) +
         gini(exclDist,exclW) * ((double)exclW / distWeight));
      if( f>bestFitness ) { // Take split with largest fitness
        bestSplit = i;
        bestFitness = f;
      }
    }
    return bestSplit == -1
      ? Split.impossible(Utils.maxIndex(dist, random))
      : Split.exclusion(colIndex, bestSplit, bestFitness);
  }
}
