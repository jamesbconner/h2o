package hexlytics;

/** A decision tree implementation. 
 * 
 * The tree has leaf nodes and inner nodes. Inner nodes have their own
 * classifiers to determine which of their subtrees should be queried next. The
 * leaf nodes are more like a ConstClassifier always returning the same value. 
 *
 * @author peta
 */
public class BinaryTree  {
  private static final long serialVersionUID = -3955680395008977160L;

   static abstract class INode  {    
     int class_ = -1;    // A category reported by the inner node
     int navigate(double[]_) { return -1; }
     int classOf() { return class_; }
  }
  
  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {    
    LeafNode(int c)  { class_ = c; }
  }

  
  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int column, double value, int cl) { column_=column; value_=value; class_ = cl;  }
    public int navigate(double[] v)  { return v[column_]<=value_?0:1; }    
  }
  
  /** Classifies the row on the proper subtree recursively. 
   * Returns the default category of the row if its subnodes are not created.  */
  public int classify(Data d) {
    double[] v = new double[d.columns()];
    d.getRow(v);
    INode node = root_;
    while (true) {
      switch (node.navigate(v)) {
      case -1 : return node.classOf();
      case 0: node = ((Node)node).l_; break;
      case 1: node = ((Node)node).r_;
      }
    }
  }

  // Root of the tree
  private INode root_;
   
  /** Creates the decision tree from the root node.   */ 
  public BinaryTree(INode root) { root_ = root; }
}
