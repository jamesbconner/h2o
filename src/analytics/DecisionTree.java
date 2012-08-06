package analytics;

/** A decision tree implementation. 
 * 
 * The tree has leaf nodes and inner nodes. Inner nodes have their own
 * classifiers to determine which of their subtrees should be queried next. The
 * leaf nodes are more like a ConstClassifier always returning the same value. 
 *
 * @author peta
 */
public class DecisionTree implements Classifier {
  private static final long serialVersionUID = -3955680395008977160L;

  /** INode interface provides the toString() method and the classifyRecursive()
   * method that classifies the row using the correct subtree determined by the
   * node's internal classifier.   */
  static interface INode extends Classifier {    
    /** Classifies the row recursively by the correct subtree.  */
    int classifyRecursive(DataAdapter row);    
    /** Converts the node to string representation.  */
    String toString();
  }
  
  public static int nodeCount;
  
  /** Leaf node that for any row returns its the data class it belongs to. */
  public static class LeafNode implements INode {
    private static final long serialVersionUID = -2231341082792288314L;
    // which data category to return. 
    public final int class_;
    public LeafNode(int dataClass)     { this.class_ = dataClass; nodeCount++; }
    public int numClasses()            { return 1; }
    public String toString()           { return "(leaf "+class_+")"; }
    public int classify(DataAdapter _) { return class_; }
    public int classifyRecursive(DataAdapter _) { return class_; }
  }

  public static class SentinelNode implements INode {

    private final Classifier cls;
    
    public int classifyRecursive(DataAdapter row) {
      return cls.classify(row);
    }

    public int classify(DataAdapter row) {
      return cls.classify(row);
    }

    public int numClasses() {
      return cls.numClasses();
    }
    
    SentinelNode(Classifier c) {
      cls = c;
    }
  }
  
  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. 
   */
  static class Node implements INode {
    private static final long serialVersionUID = 7538428587319062732L;
    // classifier that determines which subtree to use
    public final Classifier classifier;
    public final INode[] subnodes;
    // A category that is reported by the inner node if no leaf nodes are 
    // present for it (important for partial trees)
    public final int defaultCategory;
    
    /** Creates the inner node with given classifier. 
     * 
     * @param classifier 
     */
    public Node(Classifier cl, int dC) {
      classifier = cl; defaultCategory = dC; nodeCount++;
      subnodes = new INode[classifier.numClasses()];
    }
    

    /** Classifies the row on the proper subtree recursively. 
     * 
     * Returns the default category of the row if its subnodes are not created.
     * @param row
     * @return 
     */
    public int classifyRecursive(DataAdapter row) {
      int cls = classifier.classify(row);
      return (subnodes[cls] == null) ? defaultCategory : subnodes[cls].classifyRecursive(row);
    }
    
    /** Classifies the row only on the classifier internal to the node. This
     * determines which subtree to use.  */
    public int classify(DataAdapter row) { return classifier.classify(row); }
    
    /** Returns to how many categories/subtrees the node itself classifies. 
     * 
     * @return 
     */
    public int numClasses() { return classifier.numClasses(); } // subnodes might be null
    
    /** Sets the index-th subtree to the given node.   */
    void setSubtree(int index, INode subtree) {
      assert (subnodes[index] == null); subnodes[index] = subtree;
    }
    
    /** Returns the string representation of the node. That is the contents of
     * the subtrees in parentheses.   */
    public String toString() {
      StringBuilder sb = new StringBuilder("(");
      sb.append(classifier.toString());
      for (int i = 0; i<subnodes.length; ++i) {
        sb.append(" ");
        if (subnodes[i]!= null) sb.append(subnodes[i].toString());
      }
      sb.append(")");
      return sb.toString();
    }
    
  }

  // Root of the tree
  private INode root_;
  
  /** Classifies the given row on the tree. Returns the classification of the
   * row as determined by the tree.  */
  public int classify(DataAdapter row) { 
    if (root_.classifyRecursive(row) == -1) {
      System.out.println(this.toString());
      root_.classifyRecursive(row); 
    }
    return root_.classifyRecursive(row);  }
  
  /** Returns the number of classes to which the tree classifies.  */
  public int numClasses() { throw new Error("NOT IMPLEMENTED");  }
  
  /** Creates the decision tree from the root node.   */ 
  public DecisionTree(INode root) { root_ = root; }
  
  public String toString() { return root_.toString(); }
}
