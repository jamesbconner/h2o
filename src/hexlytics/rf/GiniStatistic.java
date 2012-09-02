/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import hexlytics.rf.Data.Row;

/** Gini Statistic. 
 * 
 * The gini statistic works on the notion that each column has only N 
 * @author peta
 */
class GiniStatistic {
  final Data data_;
  final GiniStatistic parent_;
  final Column[] columns_;
  int classOf = -1;
  final double[] dist;
  
  public GiniStatistic(Data d, GiniStatistic s) {
    data_ = d; parent_ = s;
    columns_ = new Column[data_.columns()];
    int i = data_.features();
    dist = new double[d.classes()];
    while (i-- > 0) { 
      int off = data_.random().nextInt(data_.columns());
      if (columns_[off]!=null) continue;
      columns_[off] = new Column(off, data_.columnClasses(off), data_.classes());
    }
  }
  
  Column bestColumn_;
  
  int singleClass_ = -1;
  
  public void computeSplit() {
    for (int i = 0; i < dist.length; ++i)
      if (dist[i]!=0)
        if (singleClass_ < 0) {
          singleClass_ = i;
        } else {
          singleClass_ = -2;
          break;
        }
    if (singleClass_ >=0 )
      return;
    bestColumn_ = null;
    double bestFitness = -1;
    for (Column c: columns_) {
      if (c == null)
        continue;
      double f = c.calculateBinarySplitFitness();
      if (f > bestFitness) {
        bestFitness = f;
        bestColumn_ = c;
      }
    }
  }
  
  public int singleClass() {
    return singleClass_;
  }
  
  public int bestColumn() {
    return bestColumn_.column;
  }
  
  public int bestColumnSplit() {
    return bestColumn_.split_;
  }
  
  public void add(Row row) {
    dist[row.classOf()] += row.weight();
    for (Column c: columns_)
      if (c!=null)
        c.add(row);
  }
  
  
  // Column class --------------------------------------------------------------
  
  static class Column {
    final double[][] dist_;
    
    public final int column;
    
    double fitness_;
    int split_ = 0;
    
    public Column(int column, int numClasses, int dataClasses) {
      this.column = column;
      dist_ = new double[numClasses][dataClasses];
    }
    
    public void add(Row row) {
      dist_[row.getColumnClass(column)][row.classOf()] += row.weight();
    }
    
    /** This calculates the simple fitness when we use the n-way split on the
     * node.
     * @return 
     */
    public double calculateNWayFitness() {
      fitness_ = 0;
      for (double[] d: dist_)
        fitness_ += calculateGini(d);
      fitness_ = fitness_ / dist_.length;
      return fitness_;
    }
    
    /** Calculates the double split fitness, which is most similar to the 
     * numeric statistic predictor.
     * 
     * @return 
     */
    public double calculateBinarySplitFitness() {
      fitness_ = -1;
      // get the totals for all column classes
      double[] second = new double[dist_[0].length];
      double sumsecond = 0;
      for (double[] d: dist_)
        for (int i = 0; i < second.length; ++i) {
          second[i] += d[i];
          sumsecond += d[i];
        }
      // find the best one
      double sumtotal = sumsecond;
      double[] first = new double[second.length];
      double sumfirst = 0;
      int j = 0;
      for (double[] d : dist_) {
        for (int i = 0; i < d.length; ++i) {
          sumfirst += d[i];
          sumsecond -= d[i];
          first[i] += d[i];
          second[i] -= d[i];
        }
        double f = calculateGini(first) * (sumfirst / sumtotal);
        f += calculateGini(second) * (sumsecond / sumtotal);
        if (f > fitness_) {
          fitness_ = f;
          split_ = j;
        }
        ++j;
      }
      return fitness_;
    }

    protected static double calculateGini(double[] dist) {
      double sum = Utils.sum(dist);
      double result = 1;
      for (double d: dist)
        result -= (d/sum) * (d/sum);
      return result;
    }
  }
  
}



