package analytics;

/**
 *
 * @author peta
 */
public class EntropyStatistic extends ColumnStatistic {

  /// Number of classes the data can be classified to
  public final int numClasses;
  
  /// Numbers of splits for this classifier
  public final int numSplits;
  
  public void addDataPoint(DataAdapter row, long[] data, int offset) {
    int cls = row.dataClass();
    cls *= numSplits*4;
    int split = row.toInt(column);
    addInteger(1,data,offset+cls+split);
  }

  @Override public int dataSize() {
    return numClasses*numSplits+4;
  }
  
  public EntropyStatistic(int column, int numClasses, int numSplits) {
    super(column);
    this.numClasses = numClasses;
    this.numSplits = numSplits;
  }

  
  public Classifier createClassifier(long[] data, int offset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  
  public double fitness(long[] data, int offset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  
  
}
