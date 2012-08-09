package hexlytics;

import hexlytics.Data.Int;
import analytics.Utils;

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
    data_=data.shrinkWrap(); data_.freeze(); 
    if(verbose) System.out.println("Input data \n"+data_+"\n");
  }
    

  public void compute() { 
    long t = System.currentTimeMillis(); 
    Data sample =  data_.sampleWithReplacement(BAGSIZE);
    tree_ = new Root();
    compute(sample,tree_,0);    
    time_ = System.currentTimeMillis()-t;
  }
  
  
  void compute(Data d, INode n, int direction) {
    Statistic s = Statistic.make(statistic_, d);  
    Classifier c = s.classifier();
    int numClasses = c.numClasses();
    
    if (numClasses==1) {
      n.set(direction, new LeafNode(c.classOf()));
    } else {
      Node nd = new Node(c.column(),c.value(),c.classOf());
      n.set(direction, nd);
      Data ld = d.filter(c,0);
      compute(ld,nd,0);
      Data rd = d.filter(c,1);
      compute(rd,nd,1);
    }
  }
  

  static abstract class INode  {    
    int class_ = -1;    // A category reported by the inner node
    int navigate(double[]_) { return -1; }
    int classOf() { return class_; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    public int classify(double[] v) { return class_; }
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
   public int classify(double[] v) { return navigate(v)==0? l_.classify(v) : r_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
 }
  
 static class Root extends Node {
   Root() { super(-1,0,-1); }
   public int navigate(double[]_) { return 0; }
   public int classify(double[] v) { return l_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else throw new Error("Unsupported"); }
 }
  
  
  double BAGSIZE = 0.70;
   

  /** OutOfBag computation on a subset of rows for parallel execution. 
   * 
   */
  class OOBProcess extends Thread {

    double err = 0;
    double oobc = 0;
    
    

    public void run(INode[] trees, Data d) {

      int[] votes = new int[d.classes()];
      double[] v = new double[d.columns()];
      d.seek(0);
      for(Int it : d) {      
        d.getRow(v);
        for (int i = 0; i< votes.length; ++i) votes[i] = 0;
        int cnt = 0;
        for (INode t : trees) {
          votes[t.classify(v)]++;
          cnt++;
        }
        if (cnt==0) continue; // don't count training data
        oobc += d.weight();
        if (Utils.maxIndex(votes, d.random_) != d.classOf())  err += d.weight();
      }
    }
    
  }
  
 
  
}


