/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.data;

import java.util.Random;

/** This is a basic Data adapter core. Any particular data adapter must inherit
 * from this class. 
 *
 * @author peta
 */
public abstract class CustomDataAdapter {
  
  
  public abstract int rows();
  public abstract int features();
  public abstract int columns();
  public abstract int classes();
  public abstract String[] columnNames();
  public abstract String classColumnName();
  public abstract String name();
  public abstract String colName(int c);
  public abstract int classOf(int rowIndex);
  public abstract int getI(int colIndex, int rowIndex);
  public abstract double getD(int colIndex, int rowIndex);
  
  /** Fills the complete row to the given double index. (or as many columns
   * starting from 0 as the size of the array is. 
   * 
   * This is a trivial implemenation and might likely be overriden in the child
   * classes.
   */
  public void getRow(int rowIndex, double[] v) {
    for(int i=0;i<v.length;i++) v[i] = getD(i,rowIndex);
  }
  
  public final Random random;
  public final long seed;
  
  protected CustomDataAdapter(long seed) {
    random = new Random(seed);
    this.seed = seed;
  }
  
  protected CustomDataAdapter() {
    this(new Random().nextLong());
  }
  
  protected CustomDataAdapter(CustomDataAdapter from) {
    this(from.seed);
  }
  
  
}
