package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;

import java.io.Serializable;

/**
 * @author peta
 */
public class Tree implements Serializable {  
  private static final long serialVersionUID = 7669063054915148060L;
  INode tree_;
  long time_ ;
  public Tree() {}
  
  /** for given data creates a node and returns it. */
  final INode compute_(Data d, Statistic s) {
    if (s.singleClass()) return new LeafNode(s.classOf());
    Split best = s.best(d);
    if (best == null) return new LeafNode(s.classOf());
    else {
      Node nd = new Node(best.column,best.value);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[]{
          new Statistic(d.columns(),d.features(),d.classes(),d.random()),
          new Statistic(d.columns(),d.features(),d.classes(),d.random())};
      d.filter(best,res,stats);
      nd.set(0,compute_(res[0],stats[0]));
      nd.set(1,compute_(res[1],stats[1]));
      return nd;
    }
  }
  public final Tree compute(Data data) {
    long t = System.currentTimeMillis(); 
    Statistic s = new Statistic(data.columns(),data.features(),data.classes(),data.random());
    for (Row r : data) s.add(r);
    tree_ = compute_(data,s);
    time_ = System.currentTimeMillis()-t;
    System.out.println("Time: "+time_ + " Tree depth =  "+ tree_.depth()+ " leaves= "+ tree_.leaves() + " || "+  this);
    return this;
  }  
    
  public static abstract class INode  implements Serializable {    
    private static final long serialVersionUID = 4707665968083310297L;
    int navigate(Row r) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(Row r);
    public int depth()         { return 0; }
    public int leaves()        { return 1; }
  }
 
  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {     
    private static final long serialVersionUID = -4781620729751890945L;
    /** Type identifier of the node in the serialization */
    public static final byte NODE_TYPE = 0;
    /** Size of the serialized node. byte type and int class. */
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int c)            { class_ = c; }
    public int classify(Row r) { return class_; }
    public String toString()   { return "["+class_+"]"; }
  }

  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  static class Node extends INode {   
    private static final long serialVersionUID = -967861474179047605L;
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int column, double value) { column_=column; value_=value;  }
    public int navigate(Row r) { return r.getS(column_)<=value_?0:1; }
    public int classify(Row r) { return navigate(r)==0? l_.classify(r) : r_.classify(r); }
    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
    public String toString() { return column_ +"@" + Utils.p2d(value_) + " ("+l_+","+r_+")"; } 
    public int depth()        { return Math.max(l_.depth(), r_.depth()) + 1; }
    public int leaves()       { return l_.leaves() + r_.leaves(); }
  }
 
  public int classify(Row r) { return tree_.classify(r); } 
  public String toString() { return tree_.toString(); } 
}