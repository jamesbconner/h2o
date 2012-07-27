
package analytics;

/** A statistic that works on given column. 
 * 
 * TODO This class may not be needed. 
 *
 * @author peta
 */
public abstract class ColumnStatistic extends Statistic {
  
  public final int column;
  
  protected ColumnStatistic(int column) {   this.column = column;  }
}

