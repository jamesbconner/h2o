package hexlytics;

import hexlytics.Statistic.Split;
import hexlytics.data.Data;
import hexlytics.data.Data.Row;


/**
 *
 * @author peta
 */
public class RandomTree { 
  INode tree_;
  Data data_;
  long time_;
  private static String statistic_ = "Numeric"; // Default choice
  
  public RandomTree(Data data) { data_=data;  }    

  public void compute() { 
    long t = System.currentTimeMillis(); 
    tree_ = new Root();
    compute(data_,tree_,0);    
    time_ = System.currentTimeMillis()-t;
  }
  
  void compute(Data d, INode n, int direction) {
    int classOf = -1;
    for(Row r : d) 
       if (classOf==-1)
         classOf = r.classOf; 
       else
         if (classOf != r.classOf) {
           classOf = -1;
           break;
         }
    if (classOf!=-1) { 
      n.set(direction, new LeafNode(classOf));    
    } else {
      Statistic s = Statistic.make(statistic_, d);  
      Split best = s.best();
      if (best == null){
        n.set(direction, new LeafNode(s.classOf()));            
      }else{
        Node nd = new Node(best.column,best.value);
        n.set(direction, nd);
        Data[] res = new Data[2];
        d.filter(best,res);
        compute(res[0],nd,0);
        compute(res[1],nd,1);
      }
    }
  }
  
  public INode tree() { return tree_; }
  
  static abstract class INode  {    
    int navigate(double[]_) { return -1; }
    void set(int direction, INode n) { throw new Error("Unsupported"); }
    abstract int classify(double[] v);
 }
 
 /** Leaf node that for any row returns its the data class it belongs to. */
 static class LeafNode extends INode {    
    int class_ = -1;    // A category reported by the inner node
    LeafNode(int c)                 { class_ = c; }
    public int classify(double[] v) { return class_; }
    public String toString()        { return "["+class_+"]"; }
 }

 
 /** Inner node of the decision tree. Contains a list of subnodes and the
  * classifier to be used to decide which subtree to explore further. */
 static class Node extends INode {
   final int column_;
   final double value_;
   INode l_, r_;
   public Node(int column, double value) { column_=column; value_=value;  }
   public int navigate(double[] v) { return v[column_]<=value_?0:1; }
   public int classify(double[] v) { return navigate(v)==0? l_.classify(v) : r_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else r_=n; }
   public String toString() { return column_ +"@" + Utils.p2d(value_) + " ("+l_+","+r_+")"; } 
 }
  
 static class Root extends Node {
   Root() { super(-1,0); }
   public int navigate(double[]_)  { return 0; }
   public int classify(double[] v) { return l_.classify(v); }
   public void set(int direction, INode n) { if (direction==0) l_=n; else throw new Error("Unsupported"); }
   public String toString()        { return l_.toString(); }
 }
  

  public void classify(Data d, int[][] score) {
    for (Row r : d) score[r.index][tree_.classify(r.v)]++;
  }
  
  public static double score(Data d, int[][]score) {
    int right=0, wrong =0;
    for (Row r : d) {
      int[]votes = score[r.index];
      for(int i=0;i<d.classes();i++) 
        if(i==r.classOf) right+=votes[i]; else wrong+=votes[i];    
    }
    return wrong/(double)right;
  }
}


