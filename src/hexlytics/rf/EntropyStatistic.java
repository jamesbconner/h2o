 package hexlytics.rf;


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
  
  public EntropyStatistic(Data data, int features, int seed) { super(data, features, seed); }
  
  @Override protected Split columnSplit(int col, Data d, int[] dist, int distWeight) {
    int[] distL = new int[d.classes()];
    int[] distR = dist.clone();

    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < _columnDists[col].length; ++i) {
      float eL = 0, eR = 0;
      int totL = 0, totR = 0;
      for (int j = 0; j < distL.length; ++j) {
        double v = _columnDists[col][i][j];
        distL[j] += v;
        distR[j] -= v;
      }
      for (int e: distL) totL += e;
      if (totL == 0)   continue;
      for (int e: distL)
        if (e != 0)  eL -= (e/(double)totL) * Math.log(e/(double)totL);
      for (int e: distR) totR += e;
      if (totR == 0) continue;
      for (int e: distR) 
        if (e != 0) eR -= (e/(double)totR) * Math.log(e/(double)totR);
      double eReduction = 1.0 - (eL * totL + eR * totR) / (double)distWeight; 
      if (eReduction > maxReduction) {
        bestSplit = i; maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    return bestSplit == -1
      ? Split.impossible(Utils.maxIndex(dist,random))
      : Split.split(col,bestSplit,maxReduction);
  }
  
  @Override protected Split columnExclusion(int colIndex, Data d, int[] dist, int distWeight) {
    int[] leftDist = new int[d.classes()];
    int[] riteDist = dist.clone();

    double maxReduction = -1;
    int bestSplit = -1;
    for (int i = 0; i < _columnDists[colIndex].length; ++i) {
      double eLeft = 0, eRight = 0;
      int totLeft = 0, totRight = 0;
      for (int j = 0; j < leftDist.length; ++j) {
        int v = _columnDists[colIndex][i][j];
        riteDist[j] += leftDist[j];
        leftDist[j] = v;
        riteDist[j] -= v;
      }
      for (int e: leftDist) totLeft += e;
      if (totLeft == 0)  continue;
      for (int e: leftDist)
        if (e != 0)  eLeft -= (e/(double)totLeft) * Math.log(e/(double)totLeft);
      for (int e: riteDist)  totRight += e;
      if (totRight == 0)  continue;
      for (double e: riteDist)
        if (e != 0)  eRight -= (e/totRight) * Math.log(e/totRight);
      double eReduction = 1.0 - (eLeft * totLeft + eRight * totRight) /distWeight; 
      if (eReduction > maxReduction) {
        bestSplit = i;  maxReduction = eReduction;
      }
    }
    // no suitable split can be made, return an impossible const split
    return bestSplit == -1
      ? Split.impossible(Utils.maxIndex(dist,random))
      : Split.exclusion(colIndex,bestSplit,maxReduction);
  } 
} 
