package analytics;

/** This interface is used by the analytics access a row of data. 
 * 
 * This is important for the classifiers who understand the best the integer
 * (and to lesser extent the double data). 
 * @author peta
 */
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
  
}
