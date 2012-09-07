package hexlytics.rf;

import hexlytics.rf.Data.Row;


/** This is an entropy statistic calculation. 
 * 
 * @author peta
 */
class EntropyStatistic {
  
  
  
  
  
}



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
    classes_= data_.classes();
    dists = new double[classes_];
    
    int features = data_.features();
    int total = data_.columns();
    int[] columnsToUse = new int[features];
    int i = 0;
    for(; i < features; ++i) columnsToUse[i] = i;
    for(; i < total; ++i) {
      int o = d.random().nextInt(i);
      if( o < features ) columnsToUse[o] = i;
    }
    
    columns_ = new Column[features];
    for (i = 0; i < features; ++i) {
      int col = columnsToUse[i];
      int last = parent_ == null ? data_.last(col) : parent_.last(col);
      columns_[i] = new Column(col, last+1, classes_);
    }
  }
  
  public Split best() {  
    if (best!=null) return best;
    for (Column c: columns_) {
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
  
  public double error() {
    if (classOf == -1)
      classOf();
    double total = Utils.sum(dists);
    double others = total - dists[classOf];
    return others / total;
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
  static class Column {
    final int column; // column
    final int[]   cnt;
    final int[][] val;
    final double[][] dists;
    int first=-1, last=-1;

    Column(int c, int lst, int classes) {
      column = c; 
      cnt = new int[lst];
      val = new int[lst][classes];
      dists = new double[2][classes]; // 2 x numClasses
    }    

    void add(int class_,int o) {
      dists[1][class_]++;
      val  [o][class_]++;
      cnt  [o]++;
      if (first==-1 || first>o) first=o;
      if (last<o) last=o;
   }        
     
    Split split() {
      if (first==last) return null;
      double fit = Utils.entropyOverColumns(dists); //compute fitness with no prediction
      // now try all the possible splits
      double bestFit = -Double.MAX_VALUE;
      double split = 0, gain = 0;
      for(int i = first; i < last; i++){ // splits are between values
        if( cnt[i] == 0 ) continue;
        int len = dists[0].length;
        for( int j = 0; j < len; j++ ) {
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

  private int last(int col) {
    for( Column c : columns_ ) {
      if( c.column == col ) return c.last;
    }
    if( parent_ != null ) return parent_.last(col);
    return data_.last(col);
  }

  public void add(Row r) {
    dists[r.classOf()]++;
    for( Column c : columns_ ) c.add(r.classOf(), r.getS(c.column));
  }
 }
  