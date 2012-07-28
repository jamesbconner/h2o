/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

/**
 *
 * @author peta
 */
public class GiniStatistic extends DistributionStatistic {

  int value_ = -1;
  double fitness_;
  
  int seenType_ = -1;
  
  
  public GiniStatistic(int column, int categories, int dataCategories) {
    super(column,categories,dataCategories);
  }
  
  @Override public Classifier createClassifier(long[] data, int offset) {
    if (value_ == -1)
      computeBestSplit(data,offset);
    if (seenType_ == -2) { 
      // if we have only seen one type of the 
      return new ColumnBinaryClassifier(column,value_);
    } else { 
      // return a contant classifier
      assert (seenType_!=-1); // make sure we have seen at least a single data point
      return new Classifier.Const(seenType_); 
    }
  }

  @Override public double fitness(long[] data, int offset) {
    if (value_ == -1)
      computeBestSplit(data,offset);
    return fitness_;
  }
  
  @Override public void addDataPoint(DataAdapter row, long[] data, int offset) {
    super.addDataPoint(row,data,offset);
    if (seenType_ == -1)
      seenType_ = row.dataClass();
    else if (seenType_ != row.dataClass())
      seenType_ = -2;
  }
  
  
  protected double computeGiniOnArray(int[] arr) {
    double result = 1;
    int total = 0;
    for (int i : arr)
      total += i;
    for (int i : arr) 
      result -= ((double)i/total) * ((double)i/total);
    return result;
  }
  
  protected double computeGiniOnCategory(int cat, long[] data, int offset) {
    int[] dother = new int[dataCategories];
    int[] dcol = new int[dataCategories];
    for (int i = 0; i<categories; ++i) {
      int[] x = (i == cat) ? dcol : dother;
      for (int j = 0; j<dataCategories; ++j)
        x[j] = readInteger(data,offset + (i*dataCategories+j)*4);
    }
    return computeGiniOnArray(dcol) + computeGiniOnArray(dother);
  }
  
  protected void computeBestSplit(long[] data, int offset) {
    value_ = 0;
    fitness_ = computeGiniOnCategory(0,data,offset);
    for (int i = 1; i< categories; ++i) {
      if (fitness_ == 2) // we'll never get anything better
        break;
      double f = computeGiniOnCategory(i,data,offset);
      if (f>fitness_) {
        fitness_ = f;
        value_ = i;
      }
    }
  }
}
