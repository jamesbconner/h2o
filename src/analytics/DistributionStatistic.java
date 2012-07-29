package analytics;

/**
 * @author peta
 */
public abstract class DistributionStatistic extends Statistic {

  /// Data point column the statistic is interested in
  public final int column;
  
  /// Number of categories the data point column can have
  public final int categories;
  
  /// Number of categories the data points may have 
  public final int dataCategories;
   
  /** Creates the distribution statistic for given
   * 
   * @param column
   * @param categories
   * @param dataCategories 
   */
  protected DistributionStatistic(int column, int categories, int dataCategories) {
    this.column = column;
    this.categories = categories;
    this.dataCategories = dataCategories;
  }
  
  /** Increments the counter for given column category and data category.   */
  @Override public void addDataPoint(DataAdapter da, long[] data, int offset) {
    addDouble(1, data, (da.toInt(column) * dataCategories)  + da.dataClass() * 4);
  }

  /** For each data category and column category we must remember the count. */
  @Override public int dataSize() { return categories*dataCategories*4; }
}


