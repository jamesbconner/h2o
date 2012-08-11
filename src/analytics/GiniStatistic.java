/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

// DO NOT DELETE YET, I AM KEEPING THIS FILE FOR FUTURE REFERENCE

/**
 *
 * @author peta
 */
public class GiniStatistic extends Statistic {

  
  class ColumnStatistic {
    final double[][] dist_;
    final int column_;
    
    int bestVal = 0;
    
    int maxCat = 0;
    
    public ColumnStatistic(int column, int columnCategories, int dataCategories) {
      column_ = column;
      dist_ = new double[columnCategories][dataCategories];
    }
    
    public void addRow(DataAdapter row) {
      dist_[row.toInt(column_)][row.dataClass()] += row.weight(); 
    }
    
    double fitness() {
      double bestFitness = -Double.MAX_VALUE;
      for (int i = 0; i< dist_.length; ++i) {
        double[] dcat = new double[dist_[0].length];
        double[] dother = new double[dist_[0].length];
        for (int j = 0; j < dist_.length; ++j) {
          double[] x = (i == j) ? dcat : dother;
          for (int xx = 0; xx < x.length; ++xx)
            x[xx] = dist_[j][xx];
        }
        double f = Utils.giniOnArray(dcat) + Utils.giniOnArray(dother);  
        if (f>bestFitness) {
          bestFitness = f;
          bestVal = i;
        }
      }
      return bestFitness;
    }
      
      /*
      double result = 0;
      double sum = 0;
      for (double[] d: dist_) {
        double s = Utils.sum(d);
        result += Utils.giniOnArray(d) * s;
        sum += s;
      }
      //System.out.println("  result "+result+" sum: "+sum);
      return result / sum;
    } */
    
    int numCategories() {
      return 2;
      /*
      int result = 0;
      for (int i = 0; i<dist_.length; ++i) 
        if (Utils.sum(dist_[i])!=0)
          result = i;
      result += 1;
      //System.out.println("We have "+result+" categories for column "+column_);
      return result; */
    }
    
    int isSingleCategory() {
      int result = -1;
      for (double[] d : dist_) {
        int x = -1;
        for (int i = 0; i< d.length; ++i) 
          if (d[i]!=0) {
            if (result == -1)
              result = i;
            else if (result != i)
              return -1;
          }
      }
      return result;
    }
    
    
  }
    
  public GiniStatistic(DataAdapter data, int[] colCategories) {
    columns_ = new ColumnStatistic[data.numFeatures()];
    pickColumns(data,colCategories);
  }

  private void pickColumns(DataAdapter data, int[] colCategories) {
    A: for(int i=0;i<data.numFeatures();) {
      int col = data.random_.nextInt(data.numColumns());
      columns_[i]=new ColumnStatistic(col, colCategories[col],data.numClasses());
      for(int j=0;j<i;j++) if (columns_[i].column_==columns_[j].column_) continue A;  
      i++;
    }
  }
    
    
  final ColumnStatistic[] columns_; 

  @Override public void addRow(DataAdapter row) {
    for (ColumnStatistic s: columns_)
      s.addRow(row);
    rows += 1;
  }

  @Override public Classifier createClassifier() {
    System.out.println("node : "+rows);
    int r = columns_[0].isSingleCategory();
    if (r!=-1) {
      System.out.println("  const: "+r);
      return new Classifier.Const(r);
    }
      
    ColumnStatistic bestStat = columns_[0];
    double bestFitness = -Double.MAX_VALUE;
    for (ColumnStatistic s: columns_) {
      double f = s.fitness();
      //System.out.println("Column "+s.column_+" fitness: "+f);
      if (f>bestFitness) {
        bestFitness = f;
        bestStat = s;
      }
    }
    return new ColumnEqualsClassifier(bestStat.column_, bestStat.bestVal);
  }
  
  
  int rows = 0;
  
  class ColumnEqualsClassifier implements Classifier {
    
    
    public final int column;
    
    public final int value;
    
    public int classify(DataAdapter data) {
      return data.toInt(column) == value ? 1 : 0;
    }

    public int numClasses() {
      return 2;
    }
    
    public ColumnEqualsClassifier(int column, int value) {
      this.column = column;
      this.value = value;
    }
    
  } 
  
  class ColumnClassifier implements Classifier {

    public final int numClasses;
    
    public final int column;
    
    public int classify(DataAdapter data) {
      return data.toInt(column);
    }

    public int numClasses() {
      return numClasses;
    }
    
    public ColumnClassifier(int column, int numClasses) {
      this.column = column;
      this.numClasses = numClasses;
    }
    
  }

  
  
//  int value_ = -1;
//  double fitness_;
//  int seenType_ = -1;
//  
//  public GiniStatistic(int column, int categories, int dataCategories) {
//    super(column,categories,dataCategories);
//  }
//  
//  @Override public Classifier createClassifier(long[] data, int offset) {
//    if (value_ == -1)
//      computeBestSplit(data,offset);
//    if (seenType_ == -2) { 
//      // if we have only seen one type of the 
//      return new ColumnBinaryClassifier(column,value_);
//    } else { 
//      // return a contant classifier
//      assert (seenType_!=-1); // make sure we have seen at least a single data point
//      return new Classifier.Const(seenType_); 
//    }
//  }
//  
//  @Override public void addDataPoint(DataAdapter row, long[] data, int offset) {
//    super.addDataPoint(row,data,offset);
//    if (seenType_ == -1)
//      seenType_ = row.dataClass();
//    else if (seenType_ != row.dataClass())
//      seenType_ = -2;
//  }
//  
//  
//  protected double computeGiniOnArray(double[] arr) {
//    double result = 1;
//    double total = 0;
//    for (double i : arr)
//      total += i;
//    for (double i : arr) 
//      result -= (i/total) * (i/total);
//    return result;
//  }
//  
//  protected double computeGiniOnCategory(int cat, long[] data, int offset) {
//    double[] dother = new double[dataCategories];
//    double[] dcol = new double[dataCategories];
//    for (int i = 0; i<categories; ++i) {
//      double[] x = (i == cat) ? dcol : dother;
//      for (int j = 0; j<dataCategories; ++j)
//        x[j] = readDouble(data,offset + (i*dataCategories+j)*8);
//    }
//    return computeGiniOnArray(dcol) + computeGiniOnArray(dother);
//  }
//  
//  protected void computeBestSplit(long[] data, int offset) {
//    value_ = 0;
//    fitness_ = computeGiniOnCategory(0,data,offset);
//    for (int i = 1; i< categories; ++i) {
//      if (fitness_ == 2) // we'll never get anything better
//        break;
//      double f = computeGiniOnCategory(i,data,offset);
//      if (f>fitness_) {
//        fitness_ = f;
//        value_ = i;
//      }
//    }
//  }
}
