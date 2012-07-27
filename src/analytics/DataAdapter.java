package analytics;

/** This interface is used by the analytics access a row of data. 
 * 
 * This is important for the classifiers who understand the best the integer
 * (and to lesser extent the double data). 
 * @author peta
 */
public interface DataAdapter {
  /** Returns the index-th row, or null if such row does not exist in the dataset. */
  void getRow(int index);  
  
  /** Returns the number of rows in the dataset.    */
  int numRows();    
  
  /** Returns the number of columns in each row.  */
  int numColumns();
  
  /** Returns true if the index-th column of the current row can be converted to 
   * integer. False if converted to double.
   */
  boolean isInt(int index);
  
  /** Returns the index-th column of the current row converted to integer. 
   * Doubles are rounded.
   */ 
  int toInt(int index);
  
  /** Returns the index-th column of the current row converted to double. */
  double toDouble(int index);
  
  /** Returns the index-th column as its original type.   */
  Object originals(int index); 

  /** Returns the number of classes supported by the supervised data. 
   * 
   * @return 
   */
  int numClasses();
  
  /** Returns the class of the current data row. 
   * 
   * @return 
   */
  int dataClass();
}
