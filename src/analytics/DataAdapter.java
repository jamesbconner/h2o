package analytics;

import java.util.Random;

/** This interface is used by the analytics access a row of data. 
 * 
 * This is important for the classifiers who understand the best the integer
 * (and to lesser extent the double data). 
 * @author peta
 */

// TODO if we ever use the adapters on adapters, this *SHOULD* again be an
// interface, or at least an abstract class as it used to be. 
public abstract class DataAdapter {
  
  protected int cur = -1; // cursor in the dataset

  
  /** Move the cursor to the index-th row. */
  public void seekToRow(int index) { cur = index; }
  
  /** Returns the number of rows in the dataset.    */
  public abstract int numRows();    
  
  /** Returns the number of columns in each row.  */
  public abstract int numColumns();
  
  /** Returns true if the index-th column of the current row can be converted to 
   * integer. False if converted to double.
   */
  public abstract boolean isInt(int index);
  
  /** Returns the index-th column of the current row converted to integer. 
   * Doubles are rounded.
   */ 
  public abstract int toInt(int index);
  
  /** Returns the index-th column of the current row converted to double. */
  public abstract double toDouble(int index);
  
  /** Returns the index-th column as its original type.   */
  public Object originals(int index) { return null; } 

  /** Returns the number of classes supported by the supervised data.    */
  public abstract int numClasses();
  
  /** Returns the class of the current data row.    */
  public abstract int dataClass();
  
  /** Returns the weight of the current row. Weight are used to change
   * likelihood of getting picked during resampling. */
  public double weight() { return 1.0; }
  
  /** Create a view on this data adapter. A view shares the same data
   *  but can have another cursor.  <<Unused right now, but will
   *  come in handy if we ever want to have multiple threads going
   *  through the data in parallel.>> */
  public DataAdapter view() { return this; }
  
  /** Returns a weighted and out-of-bag sample from the given data. 
   * 
   * @param data
   * @param bagSizePercent
   * @param r
   * @return 
   */
  
  public static DataAdapter weightedOutOfBagSampling(DataAdapter data, int bagSizePercent, Random r) {
    byte[] occurrences = new byte[data.numRows()];
    double[] weights = new double[data.numRows()];
    double sumWeights = 0;
    // get in the weights we already have from the data adapter we have
    for (int i = 0; i<data.numRows(); ++i) {
      data.seekToRow(i);
      weights[i] = data.weight();
      sumWeights += weights[i];
    }
    double[] probabilities = new double[data.numRows()];
    double sumProbs = 0;
    for (int i = 0; i< data.numRows(); ++i) {
      sumProbs += r.nextDouble();
      probabilities[i] = sumProbs;
    }
    Utils.normalize(probabilities, sumProbs/sumWeights);
    probabilities[probabilities.length-1] = sumWeights;
    int k = 0;
    int l = 0;
    sumProbs = 0;
    while( k < data.numRows() && l < data.numRows()) {
      assert weights[l] > 0;
      sumProbs += weights[l];
      while( k < data.numRows() && probabilities[k] <= sumProbs ){
        occurrences[l]++;
        k++;        
      }
      l++;
    }
    int sampleSize = 0;
    for( int i = 0; i < data.numRows(); i++ )
      sampleSize += (int) occurrences[i];
    int bagSize = data.numRows() * bagSizePercent / 100;
    assert (bagSize > 0 && sampleSize > 0);
    while (bagSize < sampleSize ) {
      int offset = r.nextInt(data.numRows());
      while( true ){
        if(occurrences[offset] != 0 ){
          occurrences[offset]--;
          break;
        }
        offset = (offset + 1) % data.numRows();
      }
      sampleSize--;
    }
    return new IntWeightedWrapper(data, occurrences);
  } 
  
}



class AdapterWrapper extends DataAdapter {
  protected final DataAdapter data_;

  AdapterWrapper(DataAdapter data) {
    data_ = data;
  }
  
  @Override public void seekToRow(int index) {
    data_.seekToRow(index);
  }  
  
  @Override public int numRows() {
    return data_.numRows();
  }

  @Override public int numColumns() {
    return data_.numColumns();
  }

  @Override public boolean isInt(int index) {
    return data_.isInt(index);
  }

  @Override public int toInt(int index) {
    return data_.toInt(index);
  }

  @Override public double toDouble(int index) {
    return data_.toDouble(index);
  }

  @Override public Object originals(int index) {
    return data_.originals(index);
  }

  @Override public int numClasses() {
    return data_.numClasses();
  }

  @Override public int dataClass() {
    return data_.dataClass();
  }
  
  @Override public double weight() {
    return data_.weight();
  }
  
}

class IntWeightedWrapper extends AdapterWrapper {
  
  final byte[] occurences;
  
  IntWeightedWrapper(DataAdapter data, byte[] occurences) {
    super(data);
    assert (data.numRows() == occurences.length);
    this.occurences = occurences;
  }
  
  // TODO this is to do the same thing as the old code ( the data_.weight()
  // multiplication). I am not sure it is correct though
  @Override public double weight() {
    return occurences[cur] * data_.weight();
  }
}

class OutOfBagSampler extends AdapterWrapper {
  
  final long seed_;
  Random rnd_;
  int rowOccurence_;
  final int bagSize;
  
  public OutOfBagSampler(DataAdapter data, int bagSize, long seed) {
    super(data);
    this.bagSize = bagSize;
    seed_ = seed;
    reset();
  }
  
  protected final void reset() {
    rnd_ = new Random(seed_);
    data_.seekToRow(0);
    computeRowOccurence();
  }
  
  protected void computeRowOccurence() {
    // get       
  }
  
  @Override public void seekToRow(int index) {
    if (data_.cur != index-1)
      reset();
    while (data_.cur != index) {
      data_.seekToRow(data_.cur+1);
      computeRowOccurence();
    }
  }  
}