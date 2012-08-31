package hexlytics.rf;

import hexlytics.rf.Data.Row;

import java.util.Random;


public class Statistic {
  final Data data_;
  final Statistic parent_;
  final Column[] columns_;  //columns for which the averages are computed
  private Split best;
  int classOf = -1;
  final int classes_;
  final double[] dists ;     
  
  /** Hold information about a split. */

  public static class Split {
    public final int column; public final double value, fitness;    
    Split(int column_, double splitValue, double fitness) {
      column = column_;  value = splitValue; this.fitness = fitness;
    }    
    boolean betterThan(Split other) { return other==null || fitness > other.fitness;  }    
    public String toString() {  return column+"@"+value;   }
  }


  public Statistic(Data d, Statistic s) {
    data_ = d; parent_ = s;
    columns_ = new Column[data_.columns()];
    int i = data_.features();
    classes_= data_.classes();
    dists = new double[classes_];
    while (i-- > 0) { 
      int off = data_.random().nextInt(data_.columns());
      if (columns_[off]!=null) continue;
      columns_[off] = new Column(off);
    }
  }
  
  
  public Split best() {  
    if (best!=null) return best;
    for (Column c: columns_) {
      if (c==null) continue;
      Split s = c.split();
      if (s!=null && s.betterThan(best))  best = s;
    }
    return best;
  }
  public int classOf() {
    if (classOf==-1) {
      int max =0;
      for(int i=0;i<dists.length;i++) if (dists[i]>max) { max=(int)dists[i]; classOf = i;}
    }
    return classOf; 
  }


  public boolean singleClass() {
    int cnt = 0;
    for(double d : dists)
      if (d > 0 && ++cnt > 1 ) return false;
    return cnt==1;
  }
  
  /** Compute the statistics on each column; holds the distribution of the values
   * using weights. This class is called from the main statistic for each column
   * the statistic cares about. */
  class Column {
    final int[]   cnt;
    final int[][] val;
    int first=-1, last=-1;
    final int column; // column
    final double[][] dists = new double[2][classes_]; // 2 x numClasses    

    Column(int c) { 
      column = c; 
      int lst = Statistic.this.last(column,Statistic.this) + 1;
      cnt = new int[lst];
      val = new int[lst][classes_];
    }    

    void add(int class_,int o) {
      dists[1][class_]++;
      val  [o][class_]++;
      cnt  [o]++;
      if (first==-1 || first>o) first=o;
      if (last<o) last=o;
   }        
     
    Split split() {
      double fit = Utils.entropyOverColumns(dists); //compute fitness with no prediction
      if (first==last) return null;
      // now try all the possible splits
      double bestFit = -Double.MAX_VALUE;
      double split = 0, gain = 0;
      for(int i = first; i < last; i++){ // splits are between values
        if (cnt[i]==0) continue;
        for(int j=0;j<classes_;j++) {
          dists[0][j] += val[i][j];
          dists[1][j] -= val[i][j];
        }           
        gain = Utils.entropyCondOverRows(dists); // fitness gain
        double newFit = fit - gain; // fitness gain
        if (newFit > bestFit) { bestFit = newFit; split = i + 0.5; }
      }
      return new Split(column,split,bestFit); 
    } 
  }


  private int last(int col, Statistic start) {     
    if (this!= start && columns_[col]!=null) return columns_[col].last;
    if (parent_!=null) return parent_.last(col, start);
    return data_.last(col);
  }
  public void add(Row r) {
    dists[r.classOf()] ++;
    for (Column c : columns_) 
      if (c!=null) c.add(r.classOf(),r.getS(c.column));   
  }
 }
  