package hexlytics;

import hexlytics.DecisionTree.INode;
import hexlytics.DecisionTree.LeafNode;
import hexlytics.DecisionTree.Node;
import hexlytics.DecisionTree.SentinelNode;

import java.text.DecimalFormat;
import java.util.Random;



/**
 *
 * @author peta
 */
public class RF { 
  private DecisionTree[] trees_;
  final Data data_;
  final int numTrees_;
  long time_;
  public ProtoTree[] trees;
  Partition partition_;
  private static String statistic_ = "Numeric"; // Default choice

  
  public RF(Data data, int numtrees) { 
    data_=data; numTrees_=numtrees; 
  }
  
  public int classify(Data data) {
    int[] counts = new int[numClasses()];
    for (DecisionTree tree: trees_)
      counts[tree.classify(data)] += 1;
    return Utils.maxIndex(counts, data_.random_);
  }

  public int numClasses() { return trees_[0].numClasses(); }
  
  public void compute() { 
    long t1 = System.currentTimeMillis();
    trees_ = compute(numTrees_);
    long t2 = System.currentTimeMillis();
    time_ = (t2-t1);
  }
  public int numTrees() { return trees_.length; }
  public DecisionTree tree(int n) { return trees_[n];  }

  static final DecimalFormat df = new  DecimalFormat ("0.###");
 
  public String toString() {
    String errors="";
    for (int i = 0; i<numTrees(); ++i) 
       errors +=" " +  df.format(Classifier.Operations.error(tree(i),data_));
    return "RF:  " + trees_.length + " trees, seed="+ data_.SEED_ +", compute(ms)="+time_+"\n"
        + "#nodes="+ DecisionTree.nodeCount + "\n"
        + "OOB err = " + outOfBagError() + "\n";// + "Single tree errors: " + errors;
  }

  
  // node under construction ---------------------------------------------------

  /**
   * Describes the node that is under construction. The node has a list of all
   * statistics that must be computed for the node.
   */
  public static class ProtoNode {

    protected  Statistic statistic_; // the statistic to be computed

    ProtoNode(Statistic s) { 
       statistic_ = s; 
    }

    /**
     * Returns the normal node that should be created from the node under
     * construction. Determines the best statistic for the node based on their
     * ordering and creates its classifier which is in turn used to produce the
     * proper node.  */
    DecisionTree.INode createNode() {
      int defaultCategory = statistic_.defaultCategory();
      Classifier nc = statistic_.createClassifier();
//      statistic_=null; // Jan -- is this enough to allow GC?
      if (nc == null)
        return null;
      if (nc instanceof Classifier.Const) 
        return new LeafNode(nc.classify(null));
      Node n = new Node(nc,defaultCategory);
      for (int i = 0; i < n.subnodes.length; ++i)
        n.subnodes[i] = new SentinelNode(statistic_.createTemporaryClassifier(i));
      return n;
//      return nc instanceof Classifier.Const ? new LeafNode(nc.classify(null))
//          : new Node(nc,defaultCategory);
    }

  }

  // tree under construction ---------------------------------------------------

  /**
   * Decision tree currently under construction. Contains both the already
   * finished parts of the decision tree and the level that is currently under
   * construction.
   */
  public static class ProtoTree {

    INode[] lastNodes_ = null;
    int[] lastOffsets_ = null;
    ProtoNode[] nodes_ = null;
    int level_ = 0;
    public INode root_ = null;
    // random generator unique to the tree.
    Random rnd = null;
    // random seed used to generate the random, therefore we can always reset it
    final long seed;
    
    final Data data_;
    
    int rowIndex_ = 0;

    /**
     * Creates the tree under construction.
     * 
     * Initializes the seed from the parent
     */
    public ProtoTree(Data data) {
      data_ = data;
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
        nodes_[i] =  new ProtoNode(Statistic.make(statistic_,data_));
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

  /** A simple runnable for parallel execution. Computes n trees. 
   * 
   */
  class BuilderProcess implements Runnable {

    public final int treeStart;
    public final int numTrees;
    public final Data localData; 
    
    
    public BuilderProcess(int treeStart, int numTrees, Data data) {
      this.treeStart = treeStart;
      this.numTrees = Math.min(numTrees, trees.length-treeStart);
      this.localData = data.view();
    }
    
    public void run() {
      if (numTrees == 0)
        return;
      for( int i = treeStart; i < treeStart+numTrees; ++i )
        trees[i] = new ProtoTree(localData);
      while (true) {
        boolean done = true;
        System.out.print(".");
        for (int t = treeStart; t<treeStart+numTrees; ++t) {
           ProtoTree tree = trees[t];
           for (int r = 0; r < localData.rows(); ++r) {
   if(true) throw new Error("unimplemented"); int occurs = 0;//partition_.occurrences(t, r);
             if (occurs == 0)  continue;
             int node = partition_.getNode(t, tree.rowIndex_);
             if( node != -1 ){ // the row is still not classified completely
               localData.seek(r);
               node = tree.getNodeNumber(node);
               if( node != -1 ){
                 ProtoNode n = tree.nodes_[node];
                 for( int cnt = 0; cnt < occurs; cnt++ ) n.statistic_.addRow(localData);
               }
               partition_.setNode(t, tree.rowIndex_, node);
             }
             tree.rowIndex_++;
           }
           tree.createNextLevel();
           // the tree has been done, we may upgrade it to next level
           if( tree.nodes_ != null ) done = false;
         }
        if (done) break;
       }
    }
  }
  
  
  
  
  /**
   * Computes n random decision trees and returns them as a random forest.
   */
  DecisionTree[] compute(int numTrees) {
    int cores =  Runtime.getRuntime().availableProcessors();
    int tpc = numTrees / cores;
    if (tpc == 0)
      tpc = numTrees; 
    int rem = numTrees - tpc*cores;
    int offset = 0;
    partition_ = new Partition(data_, numTrees); 
    //TODO SAMPLE////////////////////////////////////////////////////////////////////// 
    trees = new ProtoTree[numTrees];
    // launch the threads
    Thread[] workers = new Thread[cores];
    for (int i = 0; i<cores; ++i) {
      int size = tpc;
      if (rem>0) {
        size += 1;
        --rem;
      }
      workers[i] = new Thread(new BuilderProcess(offset,size,data_));
      offset += size;
      workers[i].start();
    }
    for (Thread t: workers)
      try { t.join(); } catch( InterruptedException ex ) { }
    // cleanup 
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
  public double outOfBagError() { return outOfBagError(trees); }
  
  /** OutOfBag computation on a subset of rows for parallel execution. 
   * 
   */
  class OOBProcess extends Thread {
    int startRow;
    int numRows;
    final Data localData;
    final ProtoTree[] ts;
    
    double err = 0;
    double oobc = 0;
    
    @Override public void run() {
      if (numRows == 0)
        return;
      int[] votes = new int[localData.classes()];
      for (int r = startRow; r < startRow+numRows; ++r) {
        localData.seek(r);
        for (int i = 0; i< votes.length; ++i)
          votes[i] = 0;
        int voteCount = 0;
        for (int t = 0; t < ts.length; ++t) {
 if(true) throw new Error("unimplemented");  //       if (partition_.occurrences(t, r) > 0) continue; // don't use training data
          votes[ts[t].root_.classifyRecursive(localData)] += 1;
          voteCount += 1;
        }
        if (voteCount==0) continue; // don't count training data
        oobc += localData.weight();
        if (Utils.maxIndex(votes, localData.random_) != localData.classOf())  err += localData.weight();
      }
    }
    
    public OOBProcess(int rowStart, int numRows, Data data, ProtoTree[] ts) {
      this.startRow = rowStart;
      this.localData = data.view();
      this.numRows = Math.min(numRows, localData.rows()-rowStart);
      this.ts = ts;
    }
    
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
    long t1 = System.currentTimeMillis();
    assert (partition_ != null && ts != null); // make sure we have already computed
    double err = 0, oobc = 0;

    int cores =  Runtime.getRuntime().availableProcessors();
    int rpc = data_.rows() / cores;
    if (rpc == 0)
      rpc = data_.rows(); 
    // launch the threads
    OOBProcess[] workers = new OOBProcess[cores];
    int rem = data_.rows() - rpc * cores;
    int offset = 0;
    for (int i = 0; i<cores; ++i) {
      int size = rpc;
      if (rem>0) {
        size += 1;
        --rem;
      }
      workers[i] = new OOBProcess(offset,size,data_,ts);
      offset += size;
      workers[i].start();
    }
    for (OOBProcess t: workers) {
      try { t.join(); } catch( InterruptedException ex ) { }
      err += t.err;
      oobc += t.oobc;
    }
    t1 = System.currentTimeMillis() - t1;
    System.out.println("OOB took "+t1+" ms");
    return err / oobc;
  }
  
}

/**
 * This class samples with replacement the input data.
 * The idea is that for each tree and each row we will have a
 * byte that tells us how many times that row appears in the sample and a byte
 * that tells on which node.
 * */
class Partition {
  /* Per-tree node id of where the row falls */
  final int[][] nodes_;
  int rows_;
  
  public Partition(Data data, int trees) {
    rows_ = data.rows();
    nodes_ = new int[trees][]; //[rows_];
  }

  public int getNode(int tree, int row) { return nodes_[tree][row];  }
  public void setNode(int tree, int row, int val) {  nodes_[tree][row] =  val;  }

}
