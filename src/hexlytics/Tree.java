package hexlytics;

import hexlytics.Statistic.Split;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;

import java.io.Serializable;


/**
 * @author peta
 */
public class Tree implements Serializable {  
  private static final long serialVersionUID = 7669063054915148060L;
  INode tree_ = null;
  long time_ = 0;
  private static String statistic_ = "Numeric"; // Default choice
  
  public Tree() {}
  
  public final Tree compute(Data data) {
//    System.out.println("Computing tree: ");
//    System.out.println("  Rows:  "+data.rows());
//    for (int i = 0; i< data.columns(); ++i) 
//      System.out.println("  Cache col "+i+": "+(data.getSortedByColumn(i) == null ? "null" : data.getSortedByColumn(i).length));
    long t = System.currentTimeMillis(); 
    /* precache - not necessary anymore, kept as comments for debugging purposes for the time being*/
//    for (int i = 0; i < data.columns(); ++i)
//      data.sort(i);
//    System.out.println("Precaching done...");
    tree_ = compute_(data);
    time_ = System.currentTimeMillis()-t;
    return this;
  }
  
  /** for given data creates a node and returns it. */
  protected final INode compute_(Data d) {
    int classOf = -1;
    for(Row r : d) 
       if (classOf==-1) classOf = r.classOf(); 
       else if (classOf != r.classOf()) {  classOf = -1; break; }

    if (classOf!=-1)  return new LeafNode(classOf);
    else {
      Statistic s = Statistic.make(statistic_, d);  
      Split best = s.best();
      if (best == null) return new LeafNode(s.classOf());
      else {
        Node nd = new Node(best.column,best.value);
        Data[] res = new Data[2];
        d.filter(best,res);
        nd.set(0,compute_(res[0]));
        nd.set(1,compute_(res[1]));
        return nd;
      }
    }
  }
  
  public INode tree() { return tree_; }
  
  public static abstract class INode  implements Serializable {    
    private static final long serialVersionUID = 4707665968083310297L;
    int navigate(double[]_) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(double[] v);
  }
 
  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {     
    private static final long serialVersionUID = -4781620729751890945L;
    /** Type identifier of the node in the serialization */
    public static final byte NODE_TYPE = 0;
    /** Size of the serialized node. byte type and int class. */
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int c)                 { class_ = c; }
    public int classify(double[] v) { return class_; }
    public String toString()        { return "["+class_+"]"; }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {   
    private static final long serialVersionUID = -967861474179047605L;
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int column, double value) { column_=column; value_=value;  }
    public int navigate(double[] v) { return v[column_]<=value_?0:1; }
    public int classify(double[] v) { return navigate(v)==0? l_.classify(v) : r_.classify(v); }
    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
    public String toString() { return column_ +"@" + Utils.p2d(value_) + " ("+l_+","+r_+")"; } 
  }
  
  public int classify(Row r) { return tree_.classify(r.v); } 

  public String toString() { return tree_.toString(); } 
}


