package hexlytics;

import hexlytics.Data.Int;

/**
 *
 * @author peta
 */
public class RF { 
  private INode tree_;
  Data data_;
  long time_;
  private static String statistic_ = "Numeric"; // Default choice
  boolean verbose = true;
  
  public RF(Data data) {  
    data_=data; 
    if(verbose) System.out.println("Input data \n"+data_.materialize()+"\n");
  }
    

  public void compute() { 
    long t = System.currentTimeMillis(); 
    tree_ = new Root();
    compute(data_,tree_,0);    
    time_ = System.currentTimeMillis()-t;
  }
  
  
  void compute(Data d, INode n, int direction) {
    Statistic s = Statistic.make(statistic_, d);  
    Classifier c = s.classifier();
    int numClasses = c.numClasses();     
    if (numClasses==1) {
      n.set(direction, new LeafNode(c.classOf()));
    } else {
      Node nd = new Node(c.column(),c.value());
      n.set(direction, nd);
      Data ld = d.filter(c,0);
      compute(ld,nd,0);
      Data rd = d.filter(c,1);
      compute(rd,nd,1);
    }
  }
  

  static abstract class INode  {    
    int navigate(double[]_) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(double[] v);
 }
 
 /** Leaf node that for any row returns its the data class it belongs to. */
 static class LeafNode extends INode {    
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int c)  { class_ = c; }
    public int classify(double[] v) { return class_; }
 }

 
 /** Inner node of the decision tree. Contains a list of subnodes and the
  * classifier to be used to decide which subtree to explore further. */
 static class Node extends INode {
   final int column_;
   final double value_;
   INode l_, r_;
   public Node(int column, double value) { column_=column; value_=value;  }
   public int navigate(double[] v)  { return v[column_]<=value_?0:1; }
   public int classify(double[] v) { return navigate(v)==0? l_.classify(v) : r_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
 }
  
 static class Root extends Node {
   Root() { super(-1,0); }
   public int navigate(double[]_) { return 0; }
   public int classify(double[] v) { return l_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else throw new Error("Unsupported"); }
 }
  

  public void classify(Data d, int[][] score) {
    d.seek(0);
    double[] v = new double[d.columns()];
    for(Int it: d) {
      d.getRow(v);
      int c = tree_.classify(v);
      score[it._][c]++;
    }    
  }
  
  public static double score(Data d, int[][]score) {
    d.seek(0);
    int right=0, wrong =0;
    for(Int it: d) {
      int c = d.classOf();
      int[]votes = score[it._];
      for(int i=0;i<d.classes();i++) 
        if(i==c) right+=votes[i]; else wrong+=votes[i];    
    }
    return wrong/(double)right;
  }
}


