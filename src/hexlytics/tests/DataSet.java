/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.tests;

import java.util.Random;

/** This is a very simple and stupid dataset. I am implementing only the
 * smallest possible functionality for it so that it matches how Jan has created
 * this package. While many of the functions are effectively missing, I am
 * trying to make sure everything we have already seen will indeed be possible.
 *
 * @author peta
 */
public abstract class DataSet {
  
  protected final Random random;
  public final long seed;
  
  protected DataSet() {
    this(new Random().nextLong());
  }
  
  protected DataSet(long seed) {
    this.seed = seed;
    random = new Random(seed);
  }
  
  public abstract long numRows();
  public abstract double[] getRow(int index);
  public abstract int getRowCategory(int index);
  public abstract String columnName(int colIndex);
  public abstract int numColumns();
  
}


