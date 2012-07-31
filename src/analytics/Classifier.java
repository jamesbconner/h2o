package analytics;

/** A classifier can simply decide on the class of the data row that is given to
 * it in the classify() method. The class is returned as an integer starting
 * from 0. 
 *
 * @author peta
 */
public interface Classifier {
  
  /** Returns the class of the current row data row. */
  int classify(DataAdapter data); 
  
  /** Returns the number of classes for this classifier. */
  int numClasses();
  
  String toString();
  
  static public class Const implements Classifier {
    final int result;
    /** Creates the constant classifier that will always return the given result.  */
    public Const(int result) { this.result = result; }
    public int classify(DataAdapter row) { return result; }
    /** The ConstClassifier always classifies to only a single class.  */
    public int numClasses() { return 1; }    
  }
  
  public static class Operations {
    
    /** Returns the standard error of the given classifier on the given dataset.
     * Note that for the RF implementation: this error does not look at Bag/OutofBag.
     * This means that for RF the error reported will be smaller than the actual
     * error rate. If we wanted a more accurate error rate we could drop the inBag
     * values.     */ 
    public static double error(Classifier c, DataAdapter d) {
      double err = 0.0;
      double wsum = 0.0;
      for (int r = 0; r < d.numRows(); ++r) {
        d.seekToRow(r);
        wsum += d.weight();
        if (d.dataClass() != c.classify(d))
          err += d.weight();
      }
      return err / wsum;
    }
    
  }
  
}




