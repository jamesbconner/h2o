/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.RFBuilder;

/**
 *
 * @author peta
 */
public interface AggregatorGlue {
  
  /** Called by the aggregator when it aggregates new data and has different
   * results.
   */
  public void onChange();
}
