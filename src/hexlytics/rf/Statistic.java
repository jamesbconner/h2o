package hexlytics.rf;

import hexlytics.rf.Data.Row;
import java.util.Arrays;

public class Statistic {
  final Data data_;
  final Statistic parent_;
  final Column[] columns_;  //columns for which the averages are computed
  private Split best;
  int classOf = -1;
  final int classes_;
  final double[] dists ;     
  final int _features;
  
  /** Hold information about a split. */
  public static class Split {
    public final int column; public final double value, fitness;    
    Split(int column_, double splitValue, double fitness) {
      column = column_;  value = splitValue; this.fitness = fitness;
    }    
    boolean betterThan(Split other) { return other==null || fitness > other.fitness;  }    
    public String toString() {  return column+"@"+value;   }
  }


  public Statistic(Data d, Statistic s, int features) {
    data_ = d; parent_ = s;
    classes_= data_.classes();
    dists = new double[classes_];
    _features = features;
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
    if (classOf == -1)  classOf();
    double total = Utils.sum(dists);
    double others = total - dists[classOf];
    return others / total;
  }


  public boolean singleClass() {
    int cnt = 0;
    for(double d : dists) if (d > 0 && ++cnt > 1 ) return false;
    return cnt==1;
  }
  
  /** Compute the statistics on each column; holds the distribution of the values
   * using weights. This class is called from the main statistic for each column
   * the statistic cares about. */
  class Column {
    final int column; // column
    final int[]   cnt; // enum(rows)
    final int[]   classes;
    final int[][] val; // enum(rows) x classes
    int first=-1, last=-1, rows;

    Column(int c, int lst, int classCnt) {
      column = c; 
      cnt = new int[lst];
      val = new int[lst][classCnt];
      classes = new int[classCnt];
    }    

    void add(int class_,int o) {
      rows++;
      val  [o][class_]++;
      cnt  [o]++;
      classes[class_]++;
      if (first==-1 || first>o) first=o;
      if (last<o) last=o;
   }     
 
    /** Textbook entropy computation, except we don't compute the parent (and instead set it to 1). */
    Split split() {
      if (first==last) return null;
      double  totparent  = rows;
      int[]  left    = new int[data_.classes()];
      int[] right    = Arrays.copyOf(classes, classes.length);
      double maxReduction = -1.0, bestSplit = -1;
      for(int i = first; i < last; i++){ // splits are between values
        if( cnt[i] == 0 ) continue;
        double  eleft = 0.0, eright =0,totleft = 0, totright = 0;
        for( int j = 0; j < left.length; j++ ) {int v=val[i][j]; left[j]+=v; right[j]-=v;}
        for(int e: left) totleft += e;
        for(int e: left)  if(e!=0) eleft -=  (e / totleft) * Math.log( e / totleft ) ;           
        for(int e: right) totright += e;
        for(int e: right)  if(e!=0) eright -=  (e / totright) * Math.log( e / totright ) ;
        double ereduction =   1 -  ( (eleft * totleft + eright * totright) /  totparent );
        if ( ereduction > maxReduction ) { bestSplit = i;  maxReduction = ereduction; }       
      }
      return new Split(column,bestSplit + 0.5,maxReduction); 
    }
  }
 
  private int last(int col) {
    for( Column c : columns_ )  if( c.column == col ) return c.last;
    if( parent_ != null ) return parent_.last(col);
    return data_.last(col);
  }

  public void add(Row r) {
    dists[r.classOf()]++;
    for( Column c : columns_ ) c.add(r.classOf(), r.getS(c.column));
  }
 }
  