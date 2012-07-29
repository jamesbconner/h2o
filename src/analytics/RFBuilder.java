package analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import analytics.DecisionTree.INode;
import analytics.DecisionTree.LeafNode;
import analytics.DecisionTree.Node;

/**
 * Class capable of building random forests.
 * 
 * @author peta
 */
public abstract class RFBuilder {

  /**
   * Creates the statistics for the node under construction. The statistics are
   * based on the list of selected columns.
   * 
   * @param node
   * @param columns
   */
  protected abstract void createStatistic(ProtoNode node, int[] columns);

  protected abstract int numberOfFeatures(ProtoNode node, ProtoTree tree);

  private final long seed;
  private final Random random;
  public ProtoTree[] trees;
  Sample partition_;
  private final DataAdapter data_;

  protected RFBuilder(long seed, DataAdapter data) {
    this.seed = seed;
    random = new Random(seed);
    data_ = data;
  }

  // node under construction ---------------------------------------------------

  /**
   * Describes the node that is under construction. The node has a list of all
   * statistics that must be computed for the node.
   */
  public class ProtoNode {

    long[] statisticsData_ = null;
    // list of all statistics that must be computed for the node
    protected final ArrayList<Statistic> statistics_ = new ArrayList();

    /**
     * Adds the given statistic to the node. All statistics associated with a
     * node under construction are computed for each row.
     * 
     * @param stat
     */
    public void addStatistic(Statistic stat) {
      statistics_.add(stat);
    }

    /**
     * Initializes the storage space required for the statistics of the given
     * node.
     */
    public void initialize() {
      int size = 0;
      for( Statistic s : statistics_ ){
        size += s.dataSize();
        size = (size + 7) & -8; // round to multiple of 8
      }
      statisticsData_ = new long[size];
    }

    /**
     * Returns the normal node that should be created from the node under
     * construction. Determines the best statistic for the node based on their
     * ordering and creates its classifier which is in turn used to produce the
     * proper node.
     * 
     * @return
     */
    INode createNode() {
      Statistic best = statistics_.get(0);
      int bestOffset = 0;
      double bestFitness = best.fitness(statisticsData_, bestOffset);
      int offset = 0 + best.dataSize();
      for( int i = 1; i < statistics_.size(); ++i ){
        double f = statistics_.get(i).fitness(statisticsData_, offset);
        if( f > bestFitness ){
          best = statistics_.get(i);
          bestOffset = offset;
          bestFitness = f;
        }
        offset += statistics_.get(i).dataSize();
      }
      Classifier nc = best.createClassifier(statisticsData_, bestOffset);
      return nc instanceof Classifier.Const ? new LeafNode(nc.classify(null))
          : new Node(nc);
    }

    /**
     * Returns the array of n randomly selected numbers from 0 to columns
     * exclusively using the random generator provided.
     * 
     * @param features
     * @param columns
     * @param random
     * @return
     */
    int[] getRandom(int features, int columns, Random random) {
      int[] cols = new int[columns];
      for( int i = 0; i < cols.length; ++i )
        cols[i] = i;
      for( int i = 0; i < features; ++i ){
        int x = random.nextInt(cols.length - i) + i;
        if( i != x ){ // swap the elements
          int s = cols[i];
          cols[i] = cols[x];
          cols[x] = s;
        }
      }
      return Arrays.copyOf(cols, features);
    }
  }

  // tree under construction ---------------------------------------------------

  /**
   * Decision tree currently under construction. Contains both the already
   * finished parts of the decision tree and the level that is currently under
   * construction.
   */
  public class ProtoTree {

    INode[] lastNodes_ = null;
    int[] lastOffsets_ = null;
    ProtoNode[] nodes_ = null;
    int level_ = 0;
    public INode root_ = null;
    // random generator unique to the tree.
    Random rnd = null;
    // random seed used to generate the random, therefore we can always reset it
    final long seed;

    /**
     * Creates the tree under construction.
     * 
     * Initializes the seed from the parent
     */
    public ProtoTree() {
      this.seed = random.nextLong();
      buildNodes(1);
    }

    protected final int updateFromLevel0() {
      root_ = nodes_[0].createNode();
      lastNodes_ = new INode[] { root_ };
      lastOffsets_ = new int[] { 0 };
      return root_.numClasses() == 1 ? 0 : root_.numClasses();
    }

    // we are a level with old nodes. What must be done is:
    // - convert the nodes under construction to normal nodes and add them
    // to their parents
    // - fill in the node offsets appropriately
    // - update the lastLevelNodes appropriately
    protected final int updateToNextLevel() {
      int newNodes = 0;
      // list of new level nodes
      INode[] levelNodes = new INode[nodes_.length];
      lastOffsets_ = new int[nodes_.length];
      int nodeIndex = 0; // to which node we are adding
      int subnodeIndex = 0; // which subtree are we setting
      for( int i = 0; i < nodes_.length; ++i ){
        // make sure that nodeIndex and subnodeIndex are set properly
        while( true ){
          if( lastNodes_[nodeIndex].numClasses() <= subnodeIndex ){
            ++nodeIndex; // move to next node
            subnodeIndex = 0; // reset subnode index
          }else if( lastNodes_[nodeIndex].numClasses() == 1 ){
            ++nodeIndex;
            assert (subnodeIndex == 0);
          }else{
            break;
          }
        }
        INode n = nodes_[i].createNode();
        // fill in the new last level nodes and offsets
        levelNodes[i] = n;
        lastOffsets_[i] = newNodes;
        // if it is not a leaf node, add the number of children to the nodes
        // to be constructed
        if( n.numClasses() > 1 ) newNodes += n.numClasses();
        // store the node to its proper position and increment the subnode index
        ((Node) lastNodes_[nodeIndex]).setSubtree(subnodeIndex, n);
        ++subnodeIndex;
      }
      // change the lastLevelNodes to the levelNodes computed
      lastNodes_ = levelNodes;
      // return the amount of nodes to be created
      return newNodes;
    }

    // Builds the numNodes of nodesUnderConstruction. These nodes are then
    // initialized to produce the
    protected final void buildNodes(int numNodes) {
      // build the new nodes under construction
      // if there are no new nodes to build, set current nodes to null
      if( numNodes == 0 ) nodes_ = null;
      else{
        nodes_ = new ProtoNode[numNodes];
        for( int i = 0; i < numNodes; ++i ){
          ProtoNode n = new ProtoNode();
          createStatistic(n, n.getRandom(numberOfFeatures(n, this),
              data_.numColumns(), random));
          n.initialize();
          nodes_[i] = n;
        }
      }
    }

    /**
     * Moves the decision tree to next level. This means that all current level
     * nodes are converted to normal nodes, these are added to the trees and new
     * current level nodes are created so that their statistics can be computed.
     */
    public void createNextLevel() {
      int newNodes = 0;
      // if nodes are null, then the tree has already decided and nothing needs
      // to be done
      if( nodes_ == null ){
        lastOffsets_ = null;
        lastNodes_ = null;
        // if we are not initializing the first level, we must convert all nodes
        // under construction to proper nodes and put them in the tree and then
        // create new nodes under construction for the next level
      }else{
        // numer of nodes to be created for the next level
        newNodes = level_ == 0 ? updateFromLevel0() : updateToNextLevel();
      }
      buildNodes(newNodes);
      // reset the random generator for the rows
      rnd = new Random(this.seed);
      ++level_;

    }

    // get node number in new level logic --------------------------------------

    /**
     * Returns the new node number for the given row. The node number is
     * calculated from the old node number and its classifier. If the oldNode is
     * -1 it means the node is no longer in the tree and should be ignored
     */
    int getNodeNumber(int oldNode) {
      // if we are already -1 ignore the row completely, it has been solved
      if( oldNode == -1 ) return -1;
      // if the lastLevelNodes are not present, we are calculating root and
      // therefore all rows are node 0
      if( lastNodes_ == null ) return 0;
      // if the lastNode is leaf, do not include the row in any further tasks
      // for this tree. It has already been decided
      if( oldNode >= lastNodes_.length ) System.out.println("error here");
      if( lastNodes_[oldNode].numClasses() == 1 ) return -1;
      // use the classifier on the node to classify the node number in the new
      // level
      return lastOffsets_[oldNode] + lastNodes_[oldNode].classify(data_);
    }
  }

  /**
   * Computes n random decision trees and returns them as a random forest.
   */
  RF compute(int numTrees) {
    partition_ = new Sample(data_, numTrees, random);
    trees = new ProtoTree[numTrees];
    for( int i = 0; i < numTrees; ++i )
      trees[i] = new ProtoTree();
    while( true ){
      boolean done = true;
      for( int t = 0; t < numTrees; ++t ){
        ProtoTree tree = trees[t];

        for( int r = 0; r < data_.numRows(); ++r ){
          int count = partition_.occurrences(t, r);
          int node = partition_.getNode(t, r);
          if( node != -1 ){ // the row is still not classified completely
            data_.seekToRow(r);
            node = tree.getNodeNumber(node);
            if( node != -1 ){
              ProtoNode n = tree.nodes_[node];
              for( int cnt = 0; cnt < count; cnt++ ){
                int offset = 0;
                for( Statistic stat : n.statistics_ ){
                  stat.addDataPoint(data_, n.statisticsData_, offset);
                  offset += (stat.dataSize() + 7) & -8; // round to multiple of
                                                        // 8
                }
              }
            }
            partition_.setNode(t, r, node);
          }
        }
        tree.createNextLevel();
        // the tree has been done, we may upgrade it to next level
        if( tree.nodes_ != null ) done = false;
      }
      if( done ) break;
    }
    DecisionTree[] rf = new DecisionTree[trees.length];
    for( int i = 0; i < rf.length; ++i )
      rf[i] = new DecisionTree(trees[i].root_);
    return new RF(rf);
  }
}

/**
 * This class samples with replacement the input data (based on the Weka class
 * Bagging.java) * The idea is that for each tree and each row we will have a
 * byte that tells us how many times that row appears in the sample and a byte
 * that tells on which node.
 * */
class Sample {
  /* Per-tree count of how many time the row occurs in the sample */
  final byte[][] occurrences_;
  /* Per-tree node id of where the row falls */
  final byte[][] nodes_;
  int bagSizePercent = 70;
  int rows_;

  public Sample(DataAdapter data, int trees, Random r) {
    rows_ = data.numRows();
    occurrences_ = new byte[trees][rows_];
    nodes_ = new byte[trees][rows_];
    for( int i = 0; i < trees; i++ )
      weightedSampling(data, r, i);
  }

  public int occurrences(int tree, int row) {
    return occurrences_[tree][row];
  }

  public int getNode(int tree, int row) {
    return nodes_[tree][row];
  }

  public void setNode(int tree, int row, int val) {
    nodes_[tree][row] = (byte) val;
  }

  double sum(double[] d) {
    double r = 0.0;
    for( int i = 0; i < d.length; i++ )
      r += d[i];
    return r;
  }

  void normalize(double[] doubles, double sum) {
    if( Double.isNaN(sum) ) throw new IllegalArgumentException("NaN.");
    if( sum == 0 ) throw new IllegalArgumentException("Zero.");
    for( int i = 0; i < doubles.length; i++ )
      doubles[i] /= sum;
  }

  void weightedSampling(DataAdapter adapt, Random random, int tree) {
    double[] weights = new double[rows_];
    for( int i = 0; i < weights.length; i++ ){
      adapt.seekToRow(i);
      weights[i] = adapt.weight();
    }
    double[] probabilities = new double[rows_];
    double sumProbs = 0, sumOfWeights = sum(weights);
    for( int i = 0; i < rows_; i++ ){
      sumProbs += random.nextDouble();
      probabilities[i] = sumProbs;
    }
    normalize(probabilities, sumProbs / sumOfWeights);

    // Make sure that rounding errors don't mess things up
    probabilities[rows_ - 1] = sumOfWeights;
    int k = 0, l = 0;
    sumProbs = 0;
    while( k < rows_ && l < rows_ ){
      if( weights[l] < 0 ) throw new IllegalArgumentException(
          "Weights have to be positive.");
      sumProbs += weights[l];
      while( k < rows_ && probabilities[k] <= sumProbs ){
        occurrences_[tree][l]++;
        k++;
      }
      l++;
    }

    int sampleSize = 0;
    for( int i = 0; i < rows_; i++ )
      sampleSize += (int) occurrences_[tree][i];
    int bagSize = rows_ * bagSizePercent / 100;
    assert (bagSize > 0 && sampleSize > 0);
    while( bagSize > sampleSize ){
      int offset = random.nextInt(rows_);
      while( true ){
        if( occurrences_[tree][offset] != 0 ){
          occurrences_[tree][offset]--;
          break;
        }
        offset = (offset + 1) % rows_;
      }
      sampleSize--;
    }
  }

  double ooberr(DecisionTree[] m_Classifiers, DataAdapter data) {
    assert (m_Classifiers.length == occurrences_.length);
    double outOfBagCount = 0.0;
    double errorSum = 0.0;
    for( int i = 0; i < rows_; i++ ){
      double vote;
      double[] votes = new double[data.numClasses()];
      data.seekToRow(i);
      // determine predictions for instance
      int voteCount = 0;
      for( int j = 0; j < m_Classifiers.length; j++ ){
        if( occurrences_[j][i] > 0 ) continue;
        voteCount++;
        double[] newProbs = distributionForInstance(data, m_Classifiers[j]);
        for( int k = 0; k < newProbs.length; k++ )
          votes[k] += newProbs[k];
      }
      // "vote"
      if( !Utils.eq(sum(votes), 0) ) Utils.normalize(votes);
      vote = Utils.maxIndex(votes); // predicted class
      // error for instance
      outOfBagCount += data.weight();
      if( vote != data.dataClass() ) errorSum += data.weight();

    }
    return errorSum / outOfBagCount;
  }

  /**
   * Predicts the class memberships for a given instance. If an instance is
   * unclassified, the returned array elements must be all zero. If the class is
   * numeric, the array must consist of only one element, which contains the
   * predicted value. Note that a classifier MUST implement either this or
   * classifyInstance().
   * 
   * @param instance
   *          the instance to be classified
   * @return an array containing the estimated membership probabilities of the
   *         test instance in each class or the numeric prediction
   */
  public double[] distributionForInstance(DataAdapter instance, DecisionTree dt) {
    double[] dist = new double[instance.numClasses()];
    double classification = dt.classify(instance);
    dist[(int) classification] = 1.0;
    return dist;
  }
}

/// NOTE: This is code from Wekka!!!
class Utils {

  /** The natural logarithm of 2. */
  public static double log2 = Math.log(2);

  /** The small deviation allowed in double comparisons. */
  public static double SMALL = 1e-6;

  /**
   * Returns the correlation coefficient of two double vectors.
   * 
   * @param y1
   *          double vector 1
   * @param y2
   *          double vector 2
   * @param n
   *          the length of two double vectors
   * @return the correlation coefficient
   */
  public static final double correlation(double y1[], double y2[], int n) {

    int i;
    double av1 = 0.0, av2 = 0.0, y11 = 0.0, y22 = 0.0, y12 = 0.0, c;

    if( n <= 1 ){ return 1.0; }
    for( i = 0; i < n; i++ ){
      av1 += y1[i];
      av2 += y2[i];
    }
    av1 /= (double) n;
    av2 /= (double) n;
    for( i = 0; i < n; i++ ){
      y11 += (y1[i] - av1) * (y1[i] - av1);
      y22 += (y2[i] - av2) * (y2[i] - av2);
      y12 += (y1[i] - av1) * (y2[i] - av2);
    }
    if( y11 * y22 == 0.0 ){
      c = 1.0;
    }else{
      c = y12 / Math.sqrt(Math.abs(y11 * y22));
    }

    return c;
  }

  /**
   * Rounds a double and converts it into String.
   * 
   * @param value
   *          the double value
   * @param afterDecimalPoint
   *          the (maximum) number of digits permitted after the decimal point
   * @return the double as a formatted string
   */
  public static/* @pure@ */String doubleToString(double value,
      int afterDecimalPoint) {

    StringBuffer stringBuffer;
    double temp;
    int dotPosition;
    long precisionValue;

    temp = value * Math.pow(10.0, afterDecimalPoint);
    if( Math.abs(temp) < Long.MAX_VALUE ){
      precisionValue = (temp > 0) ? (long) (temp + 0.5) : -(long) (Math
          .abs(temp) + 0.5);
      if( precisionValue == 0 ){
        stringBuffer = new StringBuffer(String.valueOf(0));
      }else{
        stringBuffer = new StringBuffer(String.valueOf(precisionValue));
      }
      if( afterDecimalPoint == 0 ){ return stringBuffer.toString(); }
      dotPosition = stringBuffer.length() - afterDecimalPoint;
      while( ((precisionValue < 0) && (dotPosition < 1)) || (dotPosition < 0) ){
        if( precisionValue < 0 ){
          stringBuffer.insert(1, '0');
        }else{
          stringBuffer.insert(0, '0');
        }
        dotPosition++;
      }
      stringBuffer.insert(dotPosition, '.');
      if( (precisionValue < 0) && (stringBuffer.charAt(1) == '.') ){
        stringBuffer.insert(1, '0');
      }else if( stringBuffer.charAt(0) == '.' ){
        stringBuffer.insert(0, '0');
      }
      int currentPos = stringBuffer.length() - 1;
      while( (currentPos > dotPosition)
          && (stringBuffer.charAt(currentPos) == '0') ){
        stringBuffer.setCharAt(currentPos--, ' ');
      }
      if( stringBuffer.charAt(currentPos) == '.' ){
        stringBuffer.setCharAt(currentPos, ' ');
      }

      return stringBuffer.toString().trim();
    }
    return new String("" + value);
  }

  /**
   * Rounds a double and converts it into a formatted decimal-justified String.
   * Trailing 0's are replaced with spaces.
   * 
   * @param value
   *          the double value
   * @param width
   *          the width of the string
   * @param afterDecimalPoint
   *          the number of digits after the decimal point
   * @return the double as a formatted string
   */
  public static/* @pure@ */String doubleToString(double value, int width,
      int afterDecimalPoint) {

    String tempString = doubleToString(value, afterDecimalPoint);
    char[] result;
    int dotPosition;

    if( (afterDecimalPoint >= width) || (tempString.indexOf('E') != -1) ){ // Protects
                                                                           // sci
                                                                           // notation
      return tempString;
    }

    // Initialize result
    result = new char[width];
    for( int i = 0; i < result.length; i++ ){
      result[i] = ' ';
    }

    if( afterDecimalPoint > 0 ){
      // Get position of decimal point and insert decimal point
      dotPosition = tempString.indexOf('.');
      if( dotPosition == -1 ){
        dotPosition = tempString.length();
      }else{
        result[width - afterDecimalPoint - 1] = '.';
      }
    }else{
      dotPosition = tempString.length();
    }

    int offset = width - afterDecimalPoint - dotPosition;
    if( afterDecimalPoint > 0 ){
      offset--;
    }

    // Not enough room to decimal align within the supplied width
    if( offset < 0 ){ return tempString; }

    // Copy characters before decimal point
    for( int i = 0; i < dotPosition; i++ ){
      result[offset + i] = tempString.charAt(i);
    }

    // Copy characters after decimal point
    for( int i = dotPosition + 1; i < tempString.length(); i++ ){
      result[offset + i] = tempString.charAt(i);
    }

    return new String(result);
  }


  /**
   * Computes entropy for an array of integers.
   * 
   * @param counts
   *          array of counts
   * @return - a log2 a - b log2 b - c log2 c + (a+b+c) log2 (a+b+c) when given
   *         array [a b c]
   */
  public static/* @pure@ */double info(int counts[]) {

    int total = 0;
    double x = 0;
    for( int j = 0; j < counts.length; j++ ){
      x -= xlogx(counts[j]);
      total += counts[j];
    }
    return x + xlogx(total);
  }

  /**
   * Tests if a is smaller or equal to b.
   * 
   * @param a
   *          a double
   * @param b
   *          a double
   */
  public static/* @pure@ */boolean smOrEq(double a, double b) {

    return(a - b < SMALL);
  }

  /**
   * Tests if a is greater or equal to b.
   * 
   * @param a
   *          a double
   * @param b
   *          a double
   */
  public static/* @pure@ */boolean grOrEq(double a, double b) {

    return(b - a < SMALL);
  }

  /**
   * Tests if a is smaller than b.
   * 
   * @param a
   *          a double
   * @param b
   *          a double
   */
  public static/* @pure@ */boolean sm(double a, double b) {

    return(b - a > SMALL);
  }

  /**
   * Tests if a is greater than b.
   * 
   * @param a
   *          a double
   * @param b
   *          a double
   */
  public static/* @pure@ */boolean gr(double a, double b) {

    return(a - b > SMALL);
  }

  /**
   * Returns the kth-smallest value in the array.
   * 
   * @param array
   *          the array of integers
   * @param k
   *          the value of k
   * @return the kth-smallest value
   */
  public static double kthSmallestValue(int[] array, int k) {

    int[] index = new int[array.length];

    for( int i = 0; i < index.length; i++ ){
      index[i] = i;
    }

    return array[index[select(array, index, 0, array.length - 1, k)]];
  }

  /**
   * Returns the kth-smallest value in the array
   * 
   * @param array
   *          the array of double
   * @param k
   *          the value of k
   * @return the kth-smallest value
   */
  public static double kthSmallestValue(double[] array, int k) {

    int[] index = new int[array.length];

    for( int i = 0; i < index.length; i++ ){
      index[i] = i;
    }

    return array[index[select(array, index, 0, array.length - 1, k)]];
  }

  /**
   * Returns the logarithm of a for base 2.
   * 
   * @param a
   *          a double
   * @return the logarithm for base 2
   */
  public static/* @pure@ */double log2(double a) {

    return Math.log(a) / log2;
  }

  /**
   * Returns index of maximum element in a given array of doubles. First maximum
   * is returned.
   * 
   * @param doubles
   *          the array of doubles
   * @return the index of the maximum element
   */
  public static/* @pure@ */int maxIndex(double[] doubles) {

    double maximum = 0;
    int maxIndex = 0;

    for( int i = 0; i < doubles.length; i++ ){
      if( (i == 0) || (doubles[i] > maximum) ){
        maxIndex = i;
        maximum = doubles[i];
      }
    }

    return maxIndex;
  }

  /**
   * Returns index of maximum element in a given array of integers. First
   * maximum is returned.
   * 
   * @param ints
   *          the array of integers
   * @return the index of the maximum element
   */
  public static/* @pure@ */int maxIndex(int[] ints) {

    int maximum = 0;
    int maxIndex = 0;

    for( int i = 0; i < ints.length; i++ ){
      if( (i == 0) || (ints[i] > maximum) ){
        maxIndex = i;
        maximum = ints[i];
      }
    }

    return maxIndex;
  }

  /**
   * Computes the mean for an array of doubles.
   * 
   * @param vector
   *          the array
   * @return the mean
   */
  public static/* @pure@ */double mean(double[] vector) {

    double sum = 0;

    if( vector.length == 0 ){ return 0; }
    for( int i = 0; i < vector.length; i++ ){
      sum += vector[i];
    }
    return sum / (double) vector.length;
  }

  /**
   * Returns index of minimum element in a given array of integers. First
   * minimum is returned.
   * 
   * @param ints
   *          the array of integers
   * @return the index of the minimum element
   */
  public static/* @pure@ */int minIndex(int[] ints) {

    int minimum = 0;
    int minIndex = 0;

    for( int i = 0; i < ints.length; i++ ){
      if( (i == 0) || (ints[i] < minimum) ){
        minIndex = i;
        minimum = ints[i];
      }
    }

    return minIndex;
  }

  /**
   * Returns index of minimum element in a given array of doubles. First minimum
   * is returned.
   * 
   * @param doubles
   *          the array of doubles
   * @return the index of the minimum element
   */
  public static/* @pure@ */int minIndex(double[] doubles) {

    double minimum = 0;
    int minIndex = 0;

    for( int i = 0; i < doubles.length; i++ ){
      if( (i == 0) || (doubles[i] < minimum) ){
        minIndex = i;
        minimum = doubles[i];
      }
    }

    return minIndex;
  }

  /**
   * Normalizes the doubles in the array by their sum.
   * 
   * @param doubles
   *          the array of double
   * @exception IllegalArgumentException
   *              if sum is Zero or NaN
   */
  public static void normalize(double[] doubles) {

    double sum = 0;
    for( int i = 0; i < doubles.length; i++ ){
      sum += doubles[i];
    }
    normalize(doubles, sum);
  }

  /**
   * Normalizes the doubles in the array using the given value.
   * 
   * @param doubles
   *          the array of double
   * @param sum
   *          the value by which the doubles are to be normalized
   * @exception IllegalArgumentException
   *              if sum is zero or NaN
   */
  public static void normalize(double[] doubles, double sum) {

    if( Double.isNaN(sum) ){ throw new IllegalArgumentException(
        "Can't normalize array. Sum is NaN."); }
    if( sum == 0 ){
      // Maybe this should just be a return.
      throw new IllegalArgumentException("Can't normalize array. Sum is zero.");
    }
    for( int i = 0; i < doubles.length; i++ ){
      doubles[i] /= sum;
    }
  }

  /**
   * Converts an array containing the natural logarithms of probabilities stored
   * in a vector back into probabilities. The probabilities are assumed to sum
   * to one.
   * 
   * @param a
   *          an array holding the natural logarithms of the probabilities
   * @return the converted array
   */
  public static double[] logs2probs(double[] a) {

    double max = a[maxIndex(a)];
    double sum = 0.0;

    double[] result = new double[a.length];
    for( int i = 0; i < a.length; i++ ){
      result[i] = Math.exp(a[i] - max);
      sum += result[i];
    }

    normalize(result, sum);

    return result;
  }

  /**
   * Returns the log-odds for a given probabilitiy.
   * 
   * @param prob
   *          the probabilitiy
   * 
   * @return the log-odds after the probability has been mapped to [Utils.SMALL,
   *         1-Utils.SMALL]
   */
  public static/* @pure@ */double probToLogOdds(double prob) {

    if( gr(prob, 1) || (sm(prob, 0)) ){ throw new IllegalArgumentException(
        "probToLogOdds: probability must " + "be in [0,1] " + prob); }
    double p = SMALL + (1.0 - 2 * SMALL) * prob;
    return Math.log(p / (1 - p));
  }

  /**
   * Rounds a double to the next nearest integer value. The JDK version of it
   * doesn't work properly.
   * 
   * @param value
   *          the double value
   * @return the resulting integer value
   */
  public static/* @pure@ */int round(double value) {

    int roundedValue = value > 0 ? (int) (value + 0.5) : -(int) (Math
        .abs(value) + 0.5);

    return roundedValue;
  }

  /**
   * Rounds a double to the next nearest integer value in a probabilistic
   * fashion (e.g. 0.8 has a 20% chance of being rounded down to 0 and a 80%
   * chance of being rounded up to 1). In the limit, the average of the rounded
   * numbers generated by this procedure should converge to the original double.
   * 
   * @param value
   *          the double value
   * @param rand
   *          the random number generator
   * @return the resulting integer value
   */
  public static int probRound(double value, Random rand) {

    if( value >= 0 ){
      double lower = Math.floor(value);
      double prob = value - lower;
      if( rand.nextDouble() < prob ){
        return (int) lower + 1;
      }else{
        return (int) lower;
      }
    }else{
      double lower = Math.floor(Math.abs(value));
      double prob = Math.abs(value) - lower;
      if( rand.nextDouble() < prob ){
        return -((int) lower + 1);
      }else{
        return -(int) lower;
      }
    }
  }

  /**
   * Rounds a double to the given number of decimal places.
   * 
   * @param value
   *          the double value
   * @param afterDecimalPoint
   *          the number of digits after the decimal point
   * @return the double rounded to the given precision
   */
  public static/* @pure@ */double roundDouble(double value,
      int afterDecimalPoint) {

    double mask = Math.pow(10.0, (double) afterDecimalPoint);

    return (double) (Math.round(value * mask)) / mask;
  }

  /**
   * Sorts a given array of integers in ascending order and returns an array of
   * integers with the positions of the elements of the original array in the
   * sorted array. The sort is stable. (Equal elements remain in their original
   * order.)
   * 
   * @param array
   *          this array is not changed by the method!
   * @return an array of integers with the positions in the sorted array.
   */
  public static/* @pure@ */int[] sort(int[] array) {

    int[] index = new int[array.length];
    int[] newIndex = new int[array.length];
    int[] helpIndex;
    int numEqual;

    for( int i = 0; i < index.length; i++ ){
      index[i] = i;
    }
    quickSort(array, index, 0, array.length - 1);

    // Make sort stable
    int i = 0;
    while( i < index.length ){
      numEqual = 1;
      for( int j = i + 1; ((j < index.length) && (array[index[i]] == array[index[j]])); j++ ){
        numEqual++;
      }
      if( numEqual > 1 ){
        helpIndex = new int[numEqual];
        for( int j = 0; j < numEqual; j++ ){
          helpIndex[j] = i + j;
        }
        quickSort(index, helpIndex, 0, numEqual - 1);
        for( int j = 0; j < numEqual; j++ ){
          newIndex[i + j] = index[helpIndex[j]];
        }
        i += numEqual;
      }else{
        newIndex[i] = index[i];
        i++;
      }
    }
    return newIndex;
  }

  /**
   * Sorts a given array of doubles in ascending order and returns an array of
   * integers with the positions of the elements of the original array in the
   * sorted array. NOTE THESE CHANGES: the sort is no longer stable and it
   * doesn't use safe floating-point comparisons anymore. Occurrences of
   * Double.NaN are treated as Double.MAX_VALUE
   * 
   * @param array
   *          this array is not changed by the method!
   * @return an array of integers with the positions in the sorted array.
   */
  public static/* @pure@ */int[] sort(/* @non_null@ */double[] array) {

    int[] index = new int[array.length];
    array = (double[]) array.clone();
    for( int i = 0; i < index.length; i++ ){
      index[i] = i;
      if( Double.isNaN(array[i]) ){
        array[i] = Double.MAX_VALUE;
      }
    }
    quickSort(array, index, 0, array.length - 1);
    return index;
  }

  /**
   * Tests if a is equal to b.
   * 
   * @param a
   *          a double
   * @param b
   *          a double
   */
  public static/* @pure@ */boolean eq(double a, double b) {

    return (a - b < SMALL) && (b - a < SMALL);
  }

  /**
   * Sorts a given array of doubles in ascending order and returns an array of
   * integers with the positions of the elements of the original array in the
   * sorted array. The sort is stable (Equal elements remain in their original
   * order.) Occurrences of Double.NaN are treated as Double.MAX_VALUE
   * 
   * @param array
   *          this array is not changed by the method!
   * @return an array of integers with the positions in the sorted array.
   */
  public static/* @pure@ */int[] stableSort(double[] array) {

    int[] index = new int[array.length];
    int[] newIndex = new int[array.length];
    int[] helpIndex;
    int numEqual;

    array = (double[]) array.clone();
    for( int i = 0; i < index.length; i++ ){
      index[i] = i;
      if( Double.isNaN(array[i]) ){
        array[i] = Double.MAX_VALUE;
      }
    }
    quickSort(array, index, 0, array.length - 1);

    // Make sort stable

    int i = 0;
    while( i < index.length ){
      numEqual = 1;
      for( int j = i + 1; ((j < index.length) && Utils.eq(array[index[i]],
          array[index[j]])); j++ )
        numEqual++;
      if( numEqual > 1 ){
        helpIndex = new int[numEqual];
        for( int j = 0; j < numEqual; j++ )
          helpIndex[j] = i + j;
        quickSort(index, helpIndex, 0, numEqual - 1);
        for( int j = 0; j < numEqual; j++ )
          newIndex[i + j] = index[helpIndex[j]];
        i += numEqual;
      }else{
        newIndex[i] = index[i];
        i++;
      }
    }

    return newIndex;
  }

  /**
   * Computes the variance for an array of doubles.
   * 
   * @param vector
   *          the array
   * @return the variance
   */
  public static/* @pure@ */double variance(double[] vector) {

    double sum = 0, sumSquared = 0;

    if( vector.length <= 1 ){ return 0; }
    for( int i = 0; i < vector.length; i++ ){
      sum += vector[i];
      sumSquared += (vector[i] * vector[i]);
    }
    double result = (sumSquared - (sum * sum / (double) vector.length))
        / (double) (vector.length - 1);

    // We don't like negative variance
    if( result < 0 ){
      return 0;
    }else{
      return result;
    }
  }

  /**
   * Computes the sum of the elements of an array of doubles.
   * 
   * @param doubles
   *          the array of double
   * @return the sum of the elements
   */
  public static/* @pure@ */double sum(double[] doubles) {

    double sum = 0;

    for( int i = 0; i < doubles.length; i++ ){
      sum += doubles[i];
    }
    return sum;
  }

  /**
   * Computes the sum of the elements of an array of integers.
   * 
   * @param ints
   *          the array of integers
   * @return the sum of the elements
   */
  public static/* @pure@ */int sum(int[] ints) {

    int sum = 0;

    for( int i = 0; i < ints.length; i++ ){
      sum += ints[i];
    }
    return sum;
  }

  /**
   * Returns c*log2(c) for a given integer value c.
   * 
   * @param c
   *          an integer value
   * @return c*log2(c) (but is careful to return 0 if c is 0)
   */
  public static/* @pure@ */double xlogx(int c) {

    if( c == 0 ){ return 0.0; }
    return c * Utils.log2((double) c);
  }

  /**
   * Partitions the instances around a pivot. Used by quicksort and
   * kthSmallestValue.
   * 
   * @param array
   *          the array of doubles to be sorted
   * @param index
   *          the index into the array of doubles
   * @param l
   *          the first index of the subset
   * @param r
   *          the last index of the subset
   * 
   * @return the index of the middle element
   */
  private static int partition(double[] array, int[] index, int l, int r) {

    double pivot = array[index[(l + r) / 2]];
    int help;

    while( l < r ){
      while( (array[index[l]] < pivot) && (l < r) ){
        l++;
      }
      while( (array[index[r]] > pivot) && (l < r) ){
        r--;
      }
      if( l < r ){
        help = index[l];
        index[l] = index[r];
        index[r] = help;
        l++;
        r--;
      }
    }
    if( (l == r) && (array[index[r]] > pivot) ){
      r--;
    }

    return r;
  }

  /**
   * Partitions the instances around a pivot. Used by quicksort and
   * kthSmallestValue.
   * 
   * @param array
   *          the array of integers to be sorted
   * @param index
   *          the index into the array of integers
   * @param l
   *          the first index of the subset
   * @param r
   *          the last index of the subset
   * 
   * @return the index of the middle element
   */
  private static int partition(int[] array, int[] index, int l, int r) {

    double pivot = array[index[(l + r) / 2]];
    int help;

    while( l < r ){
      while( (array[index[l]] < pivot) && (l < r) ){
        l++;
      }
      while( (array[index[r]] > pivot) && (l < r) ){
        r--;
      }
      if( l < r ){
        help = index[l];
        index[l] = index[r];
        index[r] = help;
        l++;
        r--;
      }
    }
    if( (l == r) && (array[index[r]] > pivot) ){
      r--;
    }

    return r;
  }

  /**
   * Implements quicksort according to Manber's "Introduction to Algorithms".
   * 
   * @param array
   *          the array of doubles to be sorted
   * @param index
   *          the index into the array of doubles
   * @param left
   *          the first index of the subset to be sorted
   * @param right
   *          the last index of the subset to be sorted
   */
  // @ requires 0 <= first && first <= right && right < array.length;
  // @ requires (\forall int i; 0 <= i && i < index.length; 0 <= index[i] &&
  // index[i] < array.length);
  // @ requires array != index;
  // assignable index;
  private static void quickSort(/* @non_null@ */double[] array, /* @non_null@ */
      int[] index, int left, int right) {

    if( left < right ){
      int middle = partition(array, index, left, right);
      quickSort(array, index, left, middle);
      quickSort(array, index, middle + 1, right);
    }
  }

  /**
   * Implements quicksort according to Manber's "Introduction to Algorithms".
   * 
   * @param array
   *          the array of integers to be sorted
   * @param index
   *          the index into the array of integers
   * @param left
   *          the first index of the subset to be sorted
   * @param right
   *          the last index of the subset to be sorted
   */
  // @ requires 0 <= first && first <= right && right < array.length;
  // @ requires (\forall int i; 0 <= i && i < index.length; 0 <= index[i] &&
  // index[i] < array.length);
  // @ requires array != index;
  // assignable index;
  private static void quickSort(/* @non_null@ */int[] array, /* @non_null@ */
      int[] index, int left, int right) {

    if( left < right ){
      int middle = partition(array, index, left, right);
      quickSort(array, index, left, middle);
      quickSort(array, index, middle + 1, right);
    }
  }

  /**
   * Implements computation of the kth-smallest element according to Manber's
   * "Introduction to Algorithms".
   * 
   * @param array
   *          the array of double
   * @param index
   *          the index into the array of doubles
   * @param left
   *          the first index of the subset
   * @param right
   *          the last index of the subset
   * @param k
   *          the value of k
   * 
   * @return the index of the kth-smallest element
   */
  // @ requires 0 <= first && first <= right && right < array.length;
  private static int select(/* @non_null@ */double[] array, /* @non_null@ */
      int[] index, int left, int right, int k) {

    if( left == right ){
      return left;
    }else{
      int middle = partition(array, index, left, right);
      if( (middle - left + 1) >= k ){
        return select(array, index, left, middle, k);
      }else{
        return select(array, index, middle + 1, right, k - (middle - left + 1));
      }
    }
  }

 


  /**
   * Implements computation of the kth-smallest element according to Manber's
   * "Introduction to Algorithms".
   * 
   * @param array
   *          the array of integers
   * @param index
   *          the index into the array of integers
   * @param left
   *          the first index of the subset
   * @param right
   *          the last index of the subset
   * @param k
   *          the value of k
   * 
   * @return the index of the kth-smallest element
   */
  // @ requires 0 <= first && first <= right && right < array.length;
  private static int select(/* @non_null@ */int[] array, /* @non_null@ */
      int[] index, int left, int right, int k) {

    if( left == right ){
      return left;
    }else{
      int middle = partition(array, index, left, right);
      if( (middle - left + 1) >= k ){
        return select(array, index, left, middle, k);
      }else{
        return select(array, index, middle + 1, right, k - (middle - left + 1));
      }
    }
  }

}