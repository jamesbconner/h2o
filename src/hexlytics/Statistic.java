package hexlytics;

import hexlytics.data.Data;
import hexlytics.data.Data.Row;


public class Statistic {
 
  public static Statistic make(String name, Data d) {
    if (name.equals("Numeric")) return new Statistic(d);
    else throw new Error("Unsupported stat " + name);
  }
  
  private final Data data;  // data
  private final Column[] columns_;  //columns for which the averages are computed
  double[] v_;
  /** Hold information about a split. */
  public static class Split {
    public final int column; public final double value, fitness;    
    Split(int column, double splitValue, double fitness) {
      this.column = column;  this.value = splitValue; this.fitness = fitness;
    }    
    boolean betterThan(Split other) { return other==null || fitness > other.fitness;  }
    
    public String toString() {
      return "Column: "+column+", value: "+value;
    }
  }

 
  /** Computer  the statistic on each column; holds the distribution of the values 
   * using weights. This class is called from the main statistic for each column 
   * the statistic cares about. */
  class Column {
    int column; // column
    double[][] dists; // 2 x numClasses    
    Column(int c) { column = c; dists = new double[2][data.classes()];  }    
    void add(int class_,double weight) { dists[1][class_] += weight; }    
    
    /** Calculates the best split on given column and returns its information.
     * In order to do this it must sort the rows for the given node and then
     * walk again over them in order (that is out of their natural order) to
     * determine the best splitpoint.   */
    Split split() {
      double fit = Utils.entropyOverColumns(dists); //compute fitness with no prediction
      Data sd =  data.sortByColumn(column);
      double last = sd.getRow(sd.rows()-1).v[column];
      double currSplit =  sd.getRow(0).v[column];
      if (last == currSplit) return null;
      // now try all the possible splits
      double bestFit = -Double.MAX_VALUE;
      double split = 0, gain = 0;
      for (Row r : sd){
        double s = r.v[column];
        if (s > currSplit) {
          gain = Utils.entropyCondOverRows(dists); // fitness gain
          double newFit = fit - gain; // fitness gain
          if (newFit > bestFit) { bestFit = newFit; split = (s + currSplit) / 2;
          }
        }
        currSplit = s;
        dists[0][r.classOf()] += r.weight;
        dists[1][r.classOf()] -= r.weight;
      }  
      return new Split(column,split,bestFit);
    }    
  }
  

  private Split best;
  public Split best() { return best; }
  int classOf = -1;
  public int classOf() { return classOf; }
  
  public Statistic(Data data) {
    columns_ = new Column[data.features()];
    this.data = data;    
    A: for(int i=0;i<data.features();) {
      columns_[i]=new Column(data.random().nextInt(data.columns())); // TODO: Fix to avoid throwing away columns
      for(int j=0;j<i;j++) if (columns_[i].column==columns_[j].column) continue A;  
      i++;
    }
    for (Row r : data) 
      for (Column c : columns_) 
        c.add(r.classOf(),r.weight);
    
    for (Column c: columns_) {
      Split s = c.split();
      if (s!=null && s.betterThan(best)) best = s;
    }
    
    if (best == null) {
      int[] votes = new int[data.classes()];
      for(Row r: data) votes[r.classOf()]++;
      int max = 0;
      for(int i=0;i<votes.length;i++) 
        if (votes[i]>max) { max=votes[i]; classOf = i;}
    }
  }
 }
