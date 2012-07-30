package analytics;

/** Calculates average vectors for the specified columns for each data category
 * and produces the AverageClassifier at the end. This is the basic statistic
 * to be used in numeric trees with numbers, not categories as values. This
 * statistic is not expected to be combined with any other statistics and its 
 * fitness always returns 0, the minimal fitness. 
 * 
 * It computes the vector of averages of all the columns.
 * 
 * Statistics data is for each category first the number of rows falling to that
 * category followed by sums of the respective column values.
 * 
 * @author peta
 */
public class AverageStatistic extends Statistic {

  // list of columns for which the averages are computed
  private final int[] columns_;
  
  // number of the categories the input data should be classified into
  private final int numClasses_;
  
  
  /** Creates the average statistic for given columns and number of input
   * data categories. 
   * 
   * @param columns Array of columns the statistic should compute. 
   * @param numClasses Number of final categories for the input data. 
   */
  public AverageStatistic(int[] columns, int numClasses) {
    assert (columns!=null);
    columns_ = columns;
    numClasses_ = numClasses;
  }
  
  
  /** Returns the size of the data required to store the statistic. 
   * 
   * @return Required size in bytes. 
   */
  public int dataSize() {
    // for each class compute the average (sum + # ) for all classes
    return (columns_.length * 8 + 8) * numClasses_;
  }

  /** Adds the data point. Increases the category count and adds the columns of
   * the row to the statistic. 
   * 
   * @param row Row to be added.
   * @param data Where the statistic data are stored.
   * @param offset Offset where the statistic data starts. 
   */
  public void addDataPoint(DataAdapter row, long[] data, int offset) {
    offset += (columns_.length * 8 + 8) * row.dataClass();
    double w = row.weight();
    addDouble(w, data, offset);
    offset += 8;
    for (int i = 0; i<columns_.length; ++i) {
      addDouble(row.toDouble(columns_[i]) * w, data, offset);
      offset +=8;
    }
  }

  /** Creates the classifier. If the statistic has seen only rows of one type
   * creates the ConstClassifier for that type, otherwise creates a proper
   * average classifier with the averages determined by the computed statistic.
   * 
   * @param data Where the statistic data are stored.
   * @param offset Offset to the beginning of the data. 
   * @return Classifier produced by the statistic for the given node. 
   */
  public Classifier createClassifier(long[] data, int offset) {
    AClassifier c = new AClassifier(columns_,numClasses_);
    int result = -1;
    for (int i = 0; i< numClasses_; ++i) {
      double cnt = readDouble(data,offset);
      offset += 8;
      if (cnt == 0) {
        offset += columns_.length * 8;
      } else {
        double[] avg = new double[columns_.length];
        for (int j = 0; j < columns_.length; ++j) {
          avg[j] = readDouble(data,offset) / cnt;
          offset += 8;
        }
        c.setAverage(i,avg);
        // if it is the first average, store it in
        if (result == -1)
          result = i;
        // otherwise if it is second or more, we must use proper classifier
        else if (result >=0)
          result = -2;
      }
    }
    return result>=-1 ? new Classifier.Const(result) :  c;
  }

  /** Returns the fitness of the statistics. The fitness for the numeric 
   * statistic is a little meaningless without the second pass, so we do not 
   * support a combination of statistics if one of them is numeric. 
   * 
   * TODO Do something about this, or make it permanent. 
   * 
   * @param data
   * @param offset
   * @return 
   */
  public double fitness(long[] data, int offset) {  return 0;  }
  
  

  public static class AClassifier implements Classifier {
    
    // columns to look at
    private final int[] columns_;
    
    // For each final category there is a columns size vector of doubles 
    private final double[][] averages_;
    
    /** Creates the classifier with given columns and number of classification
     * categories.   */
    public AClassifier(int[] columns, int numClasses) {
      columns_ = columns;
      averages_ = new double[numClasses][];
    }
    
    /** Sets the average vector for given category.  */
    void setAverage(int dataClass, double[] av) {
      averages_[dataClass] = av;
    }

    /** Classifies the row based on the average vectors. The final category is the
     * category to whose average the row is closest over the selected columns. 
     */
    public int classify(DataAdapter row) {
      double[] avg = new double[columns_.length];
      for (int i = 0; i<columns_.length; ++i) {
        avg[i] = row.toDouble(columns_[i]);
      }
      // now we have the vector, compare the distances to find the smallest
      int result = 0;
      double rDistance = distance(avg,averages_[0]);
      for (int i = 1; i<averages_.length; ++i) {
        double d = distance(avg,averages_[i]);
        if (d<rDistance) {
          result = i;
          rDistance = d;
        }
      }
      return result;
    }
    
    /** Returns the number of categories to which we classify.  */ 
    public int numClasses() {  return averages_.length;  }
    
    // just get the distance of two vectors 
    static double distance(double[] a, double[] b) {
      if (b == null) return Double.MAX_VALUE; // largest distance
      assert (a.length == b.length);
      double result = 0;
      for (int i = 0; i<a.length; ++i) {
        double d = a[i]-b[i];
        result += d*d;
      }
      return result;
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder("avect ");
      sb.append(Utils.join(columns_," "));
      for (int i =0; i<averages_.length; ++i) {
        sb.append(" [");
        sb.append(Utils.join(averages_[i],", "));  
        sb.append("]");
      }
      return sb.toString();
    }
  }
  
}
