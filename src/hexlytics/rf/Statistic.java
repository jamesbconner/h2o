package hexlytics.rf;

import hexlytics.rf.Data.Row;

import java.util.Random;


public class Statistic {
 
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

  public Split best(Data d_) {  
    if (best!=null) return best;
    for (Column c: columns_) {
      if (c==null) continue;
      Split s = c.split(d_);
      if (s!=null && s.betterThan(best))  best = s;
    }
    return best;
  }
  public int classOf() {
    if (classOf==-1) {
      int max =0;
      for(int i=0;i<dists.length;i++) if (dists[i]>max) { max=(int)dists[i]; classOf = i;}
    }
    //if(classOf==-1)throw new Error();
    return classOf; 
  }


  public boolean singleClass() {
    int cnt = 0;
    for(double d : dists) if (d >0 ) cnt++;
    return cnt==1;
  }
  
  /** Computer  the statistic on each column; holds the distribution of the values 
   * using weights. This class is called from the main statistic for each column 
   * the statistic cares about. */
  class Column {
    int [] cnt = new int[100];
    int[][] val = new int[100][classes_];
    int first=-1, last=-1;
    int column; // column
    double[][] dists = new double[2][classes_]; // 2 x numClasses    
    Column(int c) { column = c;  }    

    void add(int class_,int o) {
      dists[1][class_]++;  val[o][class_]++;  cnt[o]++;
      if (first==-1 || first>o) first=o;
      if (last<o) last=o;
   }        
     
    Split split(Data d_) {
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

  public Statistic(int columns, int features, int classes, Random rand) {
    columns_ = new Column[columns];
    int i = features;
    classes_= classes;
    dists = new double[classes_];
    while (i > 0) { 
      int off =rand.nextInt(columns);
      if (columns_[off]!=null) continue;
      columns_[off] = new Column(off);
      i--;
    }
  }

  public void add(Row r) {
    dists[r.classOf()] ++;
    for (Column c : columns_) 
      if (c!=null) c.add(r.classOf(),r.getS(c.column));   
  }
 }
  