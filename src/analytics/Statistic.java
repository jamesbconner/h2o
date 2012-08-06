package analytics;

/** A statistic that is capable of storing itself into the long[] arrays
 * conveniently. These long arrays are used for fast and memory efficient
 * retrieval of the statistic data by the distributed tree builder. 
 * 
 * TODO This is not thread safe yet! Locks should be implemented. 
 *
 * @author peta
 */
public abstract class Statistic {
  
  /** Adds the given row to the statistic measure.   */
  public abstract void addRow(DataAdapter adapter);
  
  /** Returns the default category for the node. Returns -1 if the current
   * statistic is not capable of computing the default category.  */
  public int defaultCategory() {  return -1; } 
  
  /** Produces the classifier from the statistic. If the statistic has seen only
   * rows of one type, the ConstClassifier should be returned.    */ 
  public abstract Classifier createClassifier();  

  public Classifier createTemporaryClassifier(int subnode) {
    return null;
  }

}
