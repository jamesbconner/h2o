package analytics;

/** A classifier can simply decide on the class of the data row that is given to
 * it in the classify() method. The class is returned as an integer starting
 * from 0. 
 *
 * @author peta
 */
public interface Classifier {
  
  /** Returns the class of the given data row. 
   * 
   * @param row
   * @return 
   */
  int classify(DataAdapter row); 
  
  /** Returns the number of classes for this classifier.
   */
  int numClasses();
  
   static public class Const implements Classifier {
    // The result for the classifier. 
    final int result;
    
    /** Creates the constant classifier that will always return the given result.
     */
    public Const(int result) { this.result = result; }
    
    /** Classifies the given row. Without touching the row, always returns the
     * preselected result. 
     */
    public int classify(DataAdapter row) { return result; }

    /** The ConstClassifier always classifies to only a single class. 
     */
    public int numClasses() { return 1; }    
  }
}
