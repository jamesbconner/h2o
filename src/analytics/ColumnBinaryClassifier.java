/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

/**
 *
 * @author peta
 */
public class ColumnBinaryClassifier implements Classifier {

  public final int column;
  public final int value;
  
  public ColumnBinaryClassifier(int column, int value) {
    this.column = column;
    this.value = value;
  }
  
   public int classify(DataAdapter row) {
    return (row.toInt(column) == value) ? 0 : 1;
  }

   public int numClasses() {
    return 2; // otherwise we won't be binary:)
  }
  
}