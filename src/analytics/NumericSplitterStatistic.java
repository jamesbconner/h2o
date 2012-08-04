/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/** Performs the split on numerical values. 
 *
 * @author peta
 */
public class NumericSplitterStatistic extends Statistic {
 
  static class SplitInfo {
    final int column;
    final double splitValue;
    final double fitness;
    
    SplitInfo(int column, double splitValue, double fitness) {
      this.column = column;
      this.splitValue = splitValue;
      this.fitness = fitness;
    }
    
    boolean betterThan(SplitInfo other) {
      return (other==null) || (fitness > other.fitness);
    }
  }
  
  
  class ColumnStatistic {
    public final byte index;
    double[][] dists;
    
    ColumnStatistic(int index, int numCategories) {
      this.index = (byte)index;
      dists = new double[2][numCategories];
    }
    
    void addDataPoint(DataAdapter row) {
      dists[1][row.dataClass()] += row.weight();
    }
    
    private double fitness(double[][] m) {
      return Utils.entropyOverColumns(m);
    }
    
    private double fitnessGain(double[][] m, double old) {
      return old - Utils.entropyCondOverRows(m);
    }

    
    class DataComparator implements Comparator<Integer> {

      @Override public int compare(Integer o1, Integer o2) {
        data.seekToRow(o1);
        double v1 = data.toDouble(index);
        data.seekToRow(o2);
        double v2 = data.toDouble(index);
        v1 = v1-v2;
        return v1== 0 ? 0 : (v1>0) ? 1 : -1 ;
      }
      
    }
    
    SplitInfo calculateBestSplit() {
      double f = fitness(dists);
      // first sort rows_ according to given column
      Collections.sort(rows_,new DataComparator());
      data.seekToRow(rows_.get(0));
      // now try all the possible splits
      double currSplit = data.toDouble(index);
      double bestFit = -Double.MAX_VALUE;
      double splitValue = 0;
      for (int i : rows_) {
        data.seekToRow(i);
        if (data.toDouble(index) > currSplit) {
          double fit = fitnessGain(dists,f);
          if (fit > bestFit) {
            bestFit = fit;
            splitValue = (data.toDouble(index) + currSplit) / 2;
          }
        }
        currSplit = data.toDouble(index);
        dists[0][data.dataClass()] += data.weight();
        dists[1][data.dataClass()] -= data.weight();
      }
      return new SplitInfo(index,splitValue,bestFit);
    }
  }
  
  
  
  /** Add the data point to all column statistics
   * 
   * @param adapter 
   */
  
  @Override public void addDataPoint(DataAdapter adapter) {
    for (ColumnStatistic s: columns_)
      s.addDataPoint(adapter);
    // add the row to the list of rows valid for this node. 
    rows_.add(adapter.rowIndex());
  }

  @Override public Classifier createClassifier() {
    //System.out.println(rows_.size());
    if (rows_.size()==0)
      throw new Error("We have size 0 node");
    // check if we have only one class
    int i = -1;
    for (int j = 0; j<columns_[0].dists[1].length;++j)
      if (columns_[0].dists[1][j]!=0)
        if (i == -1)
          i = j;
        else 
          i = -2;
    if (i > -1) {
        //System.out.println("const: "+i);
        return new Classifier.Const(i);
    }
    // for each statistic, sort the rows, find the best split
    SplitInfo best = null;
    for (ColumnStatistic s: columns_) {
      SplitInfo x = s.calculateBestSplit();
      if (x.betterThan(best))
        best = x;
    }
    //System.out.println("col: "+best.column+", split: "+best.splitValue+", fitness: "+best.fitness);
/*    if (rows_.size()==4) {
      for (ColumnStatistic s: columns_)
        System.out.println(">" + s.index);
      for (Integer ii: rows_) {
        data.seekToRow(ii);
        System.out.println("cur: "+data.rowIndex()+" col 1: "+data.toDouble(1)+" class: "+data.dataClass());
        
      }
    } */  
    return new SplitClassifier(best); 
  }

  private final ArrayList<Integer> rows_ = new ArrayList();
  
  private final DataAdapter data;
  
  
  // list of columns for which the averages are computed
  private final ColumnStatistic[] columns_;

  public NumericSplitterStatistic(DataAdapter data) {
    columns_ = new ColumnStatistic[data.numFeatures()];
    A: for(int i=0;i<data.numFeatures();) {
      columns_[i]=new ColumnStatistic(data.random_.nextInt(data.numColumns()),data.numClasses());
      for(int j=0;j<i;j++) if (columns_[i].index==columns_[j].index) continue A;  
      i++;
    }
    this.data = data;
  }
  

  static class SplitClassifier implements Classifier {

    public final int column;
    public final double splitValue;
    
    
    SplitClassifier(SplitInfo i) {
      column = i.column;
      splitValue = i.splitValue;
    }
    
    @Override public int classify(DataAdapter data) {
      return data.toDouble(column) <= splitValue ? 0 : 1;
    }

    @Override public int numClasses() {
      return 2;
    }

    public String toString() {
     return "column "+column+" value "+splitValue;
   }
    
  }
  
  
}
