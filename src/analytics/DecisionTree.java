package analytics;

/** A decission tree implementation. 
 * 
 * The tree has leaf nodes and inner nodes. Inner nodes have their own
 * classifiers to determine which of their subtrees should be queried next. The
 * leaf nodes are more like a ConstClassifier always returning the same value. 
 *
 * @author peta
 */
public class DecisionTree implements Classifier {
  
  /** INode interface provides the toString() method and the classifyRecursive()
   * method that classifies the row using the correct subtree determined by the
   * node's internal classifier.   */
  static interface INode extends Classifier {
    
    /** Classifies the row recursively by the correct subtree. 
     * 
     * @param row
     * @return 
     */
    int classifyRecursive(DataAdapter row);
    
    /** Converts the node to string representation. 
     * 
     * @return 
     */
    String toString();
  }
  
  /** Leaf node that for any row returns its the data class it belongs to.
   */
  public static class LeafNode implements INode {
    // which data category to return. 
    public final int class_;
    
    /** Creates a leaf node of a specified category. 
     * 
     * @param dataClass 
     */
    public LeafNode(int dataClass) { this.class_ = dataClass; }
    
    /** Returns always the category assigned to the leaf node. 
     * 
     * @param row
     * @return 
     */
    public int classify(DataAdapter row) { return class_; }

    /** Returns always the category assigned to the leaf node. 
     * 
     * @param row
     * @return 
     */
    public int classifyRecursive(DataAdapter row) { return class_; }
    
    /** Leaf node only classifies to single category. 
     * 
     * @return 
     */
    public int numClasses() { return 1; }
    
    /** Returns the category of the node. 
     * 
     * @return 
     */
    public String toString() { return " "+class_+" "; }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. 
   */
  static class Node implements INode {
    
    // classifier that determines which subtree to use
    public final Classifier classifier;

    // subtrees
    public final INode[] subnodes;
    
    /** Creates the inner node with given classifier. 
     * 
     * @param classifier 
     */
    public Node(Classifier classifier) {
      assert (classifier != null && classifier.numClasses()>1);
      this.classifier = classifier;
      subnodes = new INode[classifier.numClasses()];
    }

    /** Classifies the row on the proper subtree recursively. 
     * 
     * @param row
     * @return 
     */
    public int classifyRecursive(DataAdapter row) {
      return subnodes[classifier.classify(row)].classifyRecursive(row);
    }
    
    /** Classifies the row only on the classifier internal to the node. This
     * determines which subtree to use. 
     * 
     * @param row
     * @return 
     */
    public int classify(DataAdapter row) { return classifier.classify(row); }
    
    /** Returns to how many categories/subtrees the node itself classifies. 
     * 
     * @return 
     */
    public int numClasses() { return subnodes.length; }
    
    /** Sets the index-th subtree to the given node. 
     * 
     * @param index
     * @param subtree 
     */
    void setSubtree(int index, INode subtree) {
      assert (subnodes[index] == null); subnodes[index] = subtree;
    }
    
    /** Returns the string representation of the node. That is the contents of
     * the subtrees in parentheses. 
     * 
     * @return 
     */
    public String toString() {
      StringBuilder sb = new StringBuilder("(");
      for (int i = 0; i<subnodes.length; ++i) sb.append(subnodes[i].toString());
      sb.append(")");
      return sb.toString();
    }
    
  }

  // Root of the tree
  private INode root_;
  
  /** Classifies the given row on the tree. Returns the classification of the
   * row as determined by the tree.  */
  public int classify(DataAdapter row) { return root_.classifyRecursive(row);  }
  
  /** Returns the number of classes to which the tree classifies.  */
  public int numClasses() { throw new Error("NOT IMPLEMENTED");  }
  
  /** Creates the decision tree from the root node.   */ 
  public DecisionTree(INode root) { root_ = root; }
}
