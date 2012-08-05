package analytics;

import java.util.Random;

import analytics.DecisionTree.INode;
import analytics.DecisionTree.LeafNode;
import analytics.DecisionTree.Node;

/**
 * Class capable of building random forests.
 * 
 * @author peta
 */
public class RFBuilder {

  public ProtoTree[] trees;
  Sample partition_;
  private final DataAdapter data_;

  protected RFBuilder(DataAdapter data) {  data_ = data;  }
  
  // node under construction ---------------------------------------------------

  /**
   * Describes the node that is under construction. The node has a list of all
   * statistics that must be computed for the node.
   */
  public class ProtoNode {

    protected  Statistic statistic_; // the statistic to be computed

    ProtoNode(Statistic s) { 
       statistic_ = s; 
    }

    /**
     * Returns the normal node that should be created from the node under
     * construction. Determines the best statistic for the node based on their
     * ordering and creates its classifier which is in turn used to produce the
     * proper node.  */
    INode createNode() {
      int defaultCategory = statistic_.defaultCategory();
      Classifier nc = statistic_.createClassifier();
      statistic_=null; // Jan -- is this enough to allow GC?
      if (nc == null)
        return null;
      return nc instanceof Classifier.Const ? new LeafNode(nc.classify(null))
          : new Node(nc,defaultCategory);
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
    
    int rowIndex_ = 0;

    /**
     * Creates the tree under construction.
     * 
     * Initializes the seed from the parent
     */
    public ProtoTree() {
      this.seed = data_.random_.nextLong();
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
          if(( lastNodes_[nodeIndex] == null) || (lastNodes_[nodeIndex].numClasses() <= subnodeIndex )){
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
        // if not a leaf node, add the number of children to nodes to be constructed
        if(( n!=null) && (n.numClasses() > 1 )) newNodes += n.numClasses();
        // store the node to its proper position and increment the subnode index
        ((Node) lastNodes_[nodeIndex]).setSubtree(subnodeIndex, n);
        ++subnodeIndex;
      }
      // change the lastLevelNodes to the levelNodes computed
      lastNodes_ = levelNodes;
      // return the amount of nodes to be created
      return newNodes;
    }

    // Builds the numNodes of nodesUnderConstruction. 
    protected final void buildNodes(int numNodes) {
      // if there are no new nodes to build, set current nodes to null
      if( numNodes == 0 ) { nodes_ = null; return; }

      nodes_ = new ProtoNode[numNodes];
      for( int i = 0; i < numNodes; ++i )
        nodes_[i] =  new ProtoNode(data_.createStatistic());
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
      rowIndex_ = 0;

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
      if( oldNode >= lastNodes_.length ) throw new Error();
      if( lastNodes_[oldNode].numClasses() == 1 ) return -1;
      // use the classifier on the node to classify the node number in the new level
      return lastOffsets_[oldNode] + lastNodes_[oldNode].classify(data_);
    }
  }

  /**
   * Computes n random decision trees and returns them as a random forest.
   */
  DecisionTree[] compute(int numTrees) {
    partition_ = new Sample(data_, numTrees, data_.random_);
    trees = new ProtoTree[numTrees];
    for( int i = 0; i < numTrees; ++i )
      trees[i] = new ProtoTree();
    while( true ){
      boolean done = true;
      System.out.print(".");
      for( int t = 0; t < numTrees; ++t ){
        ProtoTree tree = trees[t];

        for( int r = 0; r < data_.numRows(); ++r ){
          int count = partition_.occurrences(t, r);
          if (count == 0)
            continue;
          int node = partition_.getNode(t, tree.rowIndex_);
//          System.out.println("Getting "+tree.rowIndex_+" to "+node);
//          System.out.println("old: "+node);
          if( node != -1 ){ // the row is still not classified completely
            data_.seekToRow(r);
            node = tree.getNodeNumber(node);
            if( node != -1 ){
              ProtoNode n = tree.nodes_[node];
              for( int cnt = 0; cnt < count; cnt++ )
                n.statistic_.addDataPoint(data_);
            }
 //           System.out.println(node);
 //           System.out.println("Setting "+tree.rowIndex_+" to "+node);
            partition_.setNode(t, tree.rowIndex_, node);
          }
          tree.rowIndex_++;
        }
        tree.createNextLevel();
        //System.out.println("Next level expecting nodes: " + ((tree.nodes_ == null) ? "null" : tree.nodes_.length));
        // the tree has been done, we may upgrade it to next level
        if( tree.nodes_ != null ) done = false;
      }
      if( done ) break;
    //  System.out.println("OOBE = "+outOfBagError());
    }
    //System.out.println("");
    DecisionTree[] rf = new DecisionTree[trees.length];
    for( int i = 0; i < rf.length; ++i )
      rf[i] = new DecisionTree(trees[i].root_);
    return rf;
  }
  
  /** Computes the out of bag error for the built random forest. 
   * 
   * Out classifiers are only integer and non-numeric in the final output so we
   * do not need the double vectors and their normalization. This method is thus
   * much simpler than those of different frameworks.
   * 
   * @return The out-of-bag error for the constructed tree.
   */  
  public double outOfBagError() {
    return outOfBagError(trees);
  }
  
  /** Computes the out of bag error for the built random forest. 
   * 
   * Out classifiers are only integer and non-numeric in the final output so we
   * do not need the double vectors and their normalization. This method is thus
   * much simpler than those of different frameworks.
   * 
   * @return The out-of-bag error for the constructed tree.
   */  
  public double outOfBagError(ProtoTree[] ts) {
    assert (partition_ != null && ts != null); // make sure we have already computed
    double err = 0, oobc = 0;
    
    for (int r = 0; r < data_.numRows(); ++r) {
      data_.seekToRow(r);
      int[] votes = new int[data_.numClasses()];
      int voteCount = 0;
      for (int t = 0; t < ts.length; ++t) {
        if (partition_.occurrences(t, r) > 0) continue; // don't use training data
        votes[ts[t].root_.classifyRecursive(data_)] += 1;
        voteCount += 1;
      }
      if (voteCount==0) continue; // don't count training data
      oobc += data_.weight();
      if (Utils.maxIndex(votes, data_.random_) != data_.dataClass())  err += data_.weight();
    }
    return err / oobc;
  }
  
}

/**
 * This class samples with replacement the input data.
 * The idea is that for each tree and each row we will have a
 * byte that tells us how many times that row appears in the sample and a byte
 * that tells on which node.
 * */
class Sample {
  /* Per-tree count of how many time the row occurs in the sample */
  final byte[][] occurrences_;
  /* Per-tree node id of where the row falls */
  final int[][] nodes_;
  int bagSizePercent = 70;
  int rows_;

  public Sample(DataAdapter data, int trees, Random r) {
    rows_ = data.numRows();
    occurrences_ = new byte[trees][rows_];
    nodes_ = new int[trees][]; //[rows_];
    for( int i = 0; i < trees; i++ ) weightedSampling(data, r, i);
  }

  public int occurrences(int tree, int row) { return occurrences_[tree][row]; }
  public int getNode(int tree, int row) { return nodes_[tree][row];  }
  public void setNode(int tree, int row, int val) {  nodes_[tree][row] =  val;  }
  static double sum(double[] d) {
    double r = 0.0; for( int i = 0; i < d.length; i++ ) r += d[i]; return r;
  }
  static void normalize(double[] doubles, double sum) {
    assert ! Double.isNaN(sum) && sum != 0;
    for( int i = 0; i < doubles.length; i++ )  doubles[i] /= sum;
  }

  static class WP { double [] weights, probabilities;  }
  static WP wp = new WP();
  /// TODO: if the weights change we have to recompute...
  static private WP wp(int rows_, DataAdapter adapt, Random random) {
    if(wp.weights!=null) return wp;
    wp.weights = new double[rows_];
    for( int i = 0; i < wp.weights.length; i++ ){
      adapt.seekToRow(i);
      wp.weights[i] = adapt.weight();
    }
    wp.probabilities = new double[rows_];
    double sumProbs = 0, sumOfWeights = sum(wp.weights);
    for( int i = 0; i < rows_; i++ ){
      sumProbs += random.nextDouble();
      wp.probabilities[i] = sumProbs;
    }
    normalize(wp.probabilities, sumProbs / sumOfWeights);
    wp.probabilities[rows_ - 1] = sumOfWeights;
    return wp;
  }
  

  void weightedSampling(DataAdapter adapt, Random random, int tree) {
    WP wp = wp(rows_,adapt,random);
    double[] weights = wp.weights, probabilities = wp.probabilities;
    int k = 0, l = 0, sumProbs = 0;
    while( k < rows_ && l < rows_ ){
      assert weights[l] > 0;
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
    while( bagSize < sampleSize ){
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
    int empty = 0;
    for (byte b : occurrences_[tree])
      if (b == 0) 
        empty++;
    // we can remember only the nonempty row nodes (occurrence >= 1)
    nodes_[tree] = new int[rows_-empty];
  }
  void p(String s) { System.out.println(s); }
}