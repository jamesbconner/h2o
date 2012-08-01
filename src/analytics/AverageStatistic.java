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
  private final byte[] columns_;
  
  private final double[][] sums_;
  private final double[] weights_;
  
  /** Creates the average statistic for given columns and number of input
   * data categories. 
   * 
   * @param columns Array of columns the statistic should compute. 
   * @param numClasses Number of final categories for the input data. 
   */
  public AverageStatistic(DataAdapter data) {
    
    columns_ = new byte[data.numFeatures()];
    A: for(int i=0;i<data.numFeatures();) {
      columns_[i]=(byte)data.random_.nextInt(data.numColumns());
      for(int j=0;j<i;j++) if (columns_[i]==columns_[j]) continue A;  
      i++;
    }
    sums_ = new double[data.numClasses()][columns_.length];
    weights_ = new double[data.numClasses()];
  }
  
  /** Adds the data point. Increases the category count and adds the columns of
   * the row to the statistic. 
   * 
   * @param row Row to be added.
   * @param data Where the statistic data are stored.
   * @param offset Offset where the statistic data starts. 
   */
  public void addDataPoint(DataAdapter row) {
    int idx = row.dataClass();
    weights_[idx] += row.weight();
    for (int i = 0; i<columns_.length; ++i)
      sums_[idx][i] += row.toDouble(columns_[i]);
  }
  
  /** Returns the default category - the most common answer. This is used only
   * for the partial results. 
   * 
   * @param data
   * @param offset
   * @return 
   */
  @Override public int defaultCategory() {
    return Utils.maxIndex(weights_, null);
  }

  /** Creates the classifier. If the statistic has seen only rows of one type
   * creates the ConstClassifier for that type, otherwise creates a proper
   * average classifier with the averages determined by the computed statistic.
   * 
   * @param data Where the statistic data are stored.
   * @param offset Offset to the beginning of the data. 
   * @return Classifier produced by the statistic for the given node. 
   */
  public Classifier createClassifier() {
    int result = -1;
    for (int i = 0; i < weights_.length; ++i) {
      if (weights_[i] != 0) {
        if (result == -1)
          result = i;
        else if (result>=0)
          result = -2;
        for (int j = 0; j < columns_.length; ++j) 
          sums_[i][j] /= weights_[i]; // compute the averages
      }
    }
    if (result == -1)
      return null;
    if (result >=0)
      return new Classifier.Const(result);
    return new AClassifier(columns_,sums_);
  }

  public static class AClassifier implements Classifier {
    
    private final byte[] columns_;
    
    // For each final category there is a columns size vector of doubles 
    private final double[][] averages_;
    
    public AClassifier(byte[] columns, double[][] averages) {
      columns_ = columns;
      averages_ = averages;
    }
    
    /** Sets the average vector for given category.  */
    void setAverage(int dataClass, double[] av) {  averages_[dataClass] = av;  }

    /** Classifies the row based on the average vectors. The final category is the
     * category to whose average the row is closest over the selected columns. 
     */
    public int classify(DataAdapter row) {
      double[] avg = new double[columns_.length];
      for (int i = 0; i<columns_.length; ++i) avg[i] = row.toDouble(columns_[i]);
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
    double distance(double[] a, double[] b) {
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
