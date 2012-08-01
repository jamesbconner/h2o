/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

import java.util.Random;

/**
 *
 * @author peta
 */
public class Utils {
  /** Returns the index of the largest value in the array. In case of a tie, an
   * the index is selected randomly.   */
  public static int maxIndex(int[] from, Random rand) {
    int result = 0;
    for (int i = 1; i<from.length; ++i) if (from[i]>from[result]) result = i;
      else if (from[i]==from[result] && rand.nextBoolean()) result = i; // tie breaker
    return result;
  }
  
  public static void normalize(double[] doubles, double sum) {
    assert ! Double.isNaN(sum) && sum != 0;
    for( int i = 0; i < doubles.length; i++ )  doubles[i] /= sum;
  }  
  
  public static String join(int[] what, String with) {
    if (what==null)  return "";
    StringBuilder sb = new StringBuilder();
    sb.append(what[0]);
    for (int i = 1; i<what.length;++i) sb.append(with+what[i]);
    return sb.toString();
  }


  public static String join(byte[] what, String with) {
    if (what==null)  return "";
    StringBuilder sb = new StringBuilder();
    sb.append(what[0]);
    for (int i = 1; i<what.length;++i) sb.append(with+what[i]);
    return sb.toString();
  }

  public static String join(double[] what, String with) {
    if (what==null) return "";
    StringBuilder sb = new StringBuilder();
    sb.append(what[0]);
    for (int i = 1; i<what.length;++i) sb.append(with+what[i]);
    return sb.toString();
  }
  
}
