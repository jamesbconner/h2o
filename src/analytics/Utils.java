/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package analytics;

/**
 *
 * @author peta
 */
public class Utils {
  

  
  public static int maxIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result])
        result = i;
    return result;
    
  }
  
}
