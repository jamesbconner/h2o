package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;

import java.io.Serializable;

public class Tree implements Serializable {  
  public static  int MAX_TREE_DEPTH = 20;  
  public static  double MIN_ERROR_RATE = 0.15;
  private static final long serialVersionUID = 7669063054915148060L;

  INode tree_;
  long time_ ;
  
  public Tree() {}

  final INode compute(int depth, Data d, Statistic s, Job[] jobs) {
    // terminate the branch prematurely
    if (s.classOfError() < MIN_ERROR_RATE || depth == MAX_TREE_DEPTH)  
      return new LeafNode(depth,s.classOf());
    if (s.singleClass()) return new LeafNode(depth, s.classOf());
    Split best = s.best();
    if (best == null) return new LeafNode(depth, s.classOf());
    else {
      Node nd = new Node(depth, best.column,best.value);
      Data[] res = new Data[2];
      Statistic[] stats = new Statistic[]{
          new Statistic(d,s), new Statistic(d,s)};
      d.filter(best,res,stats);
      jobs[0] = new Job(this, nd, 0,res[0],stats[0]);
      jobs[1] = new Job(this, nd, 1,res[1],stats[1]);
      return nd;
    }
  }
  
  /** Computes the tree using the gini statistic. 
   * 
   */
  final INode computeGini(int depth, Data d, GiniStatistic s, GiniJob[] jobs) {
    s.computeSplit();
    // terminate the branch prematurely
    if ((s.classOfError() < MIN_ERROR_RATE) || (depth >= MAX_TREE_DEPTH))
      return new LeafNode(depth,s.classOf());
    if (s.singleClass() >= 0)
      return new LeafNode(depth, s.singleClass());
    if (s.bestColumn_.fitness_ < 0)
      return new LeafNode(depth, s.classOf());
    GiniNode nd = new GiniNode(depth,s.bestColumn(), s.bestColumnSplit());
    Data[] res = new Data[2];
    GiniStatistic[] stats = new GiniStatistic[]{
        new GiniStatistic(d,s), new GiniStatistic(d,s)};
    d.filter(nd.column, nd.split, res, stats);
    //System.out.println(res[0].rows()+" - "+res[1].rows());
    if ((res[0].rows() == 0) || (res[1].rows()==0))
      System.out.println("a problem we have");
    jobs[0] = new GiniJob(this,nd,0,res[0],stats[0]);
    jobs[1] = new GiniJob(this,nd,1,res[1],stats[1]);
    return nd;
  }

    
  public static abstract class INode  implements Serializable {    
    int navigate(Row r) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(Row r);
    public int depth()         { return 0; }
    public int leaves()        { return 1; }
    public final int nodeDepth_;
    protected INode(int depth) {
      nodeDepth_ = depth;
    }
  }
 
  /** Leaf node that for any row returns its the data class it belongs to. */
  static class LeafNode extends INode {     
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int depth,int c) {
      super(depth);
      class_ = c;
    }
    public int classify(Row r) { return class_; }
    public String toString()   { return "["+class_+"]"; }
  }

  class GiniNode extends INode {
    final int column;
    final int split;
    INode l_, r_;

    @Override int classify(Row r) {
      return r.getColumnClass(column) <= split ? l_.classify(r) : r_.classify(r);
    }
    
    public GiniNode(int depth, int column, int split) {
      super(depth);
      this.column = column;
      this.split = split;
    }

    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
    public int depth()        { return Math.max(l_.depth(), r_.depth()) + 1; }
    public int leaves()       { return l_.leaves() + r_.leaves(); }
    
    
  }
  
  
  /** Inner node of the decision tree. Contains a list of subnodes and the
   * classifier to be used to decide which subtree to explore further. */
  class Node extends INode {   
    final int column_;
    final double value_;
    INode l_, r_;
    public Node(int depth,int column, double value) {
      super(depth);
      column_=column;
      value_=value;
    }
    public int navigate(Row r) { return r.getS(column_)<=value_?0:1; }
    public int classify(Row r) { return navigate(r)==0? l_.classify(r) : r_.classify(r); }
    public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
    public int depth()        { return Math.max(l_.depth(), r_.depth()) + 1; }
    public int leaves()       { return l_.leaves() + r_.leaves(); }
    public String toString() {
      short idx = (short)value_; // Convert split-point of the form X.5 to a (short)X
      C c = RFTask.data().data_.c_[column_];
      double dlo = c._v2o[idx+0];
      double dhi = (idx < c.sz_) ? c._v2o[idx+1] : dlo+1.0;
      double dmid = (dlo+dhi)/2.0;
      return c.name_ +"<" + Utils.p2d(dmid) + " ("+l_+","+r_+")";
    }
   }
 
  public int classify(Row r) { return tree_.classify(r); } 
  public String toString()   { return tree_.toString(); } 
}