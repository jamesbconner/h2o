/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hexlytics.rf;

import java.text.DecimalFormat;
import java.util.Random;

/**
 *
 * @author peta
 */
public class Utils {
  
  public static class MinMaxAvg {
    double min_ = Double.MAX_VALUE;
    double max_ = Double.MIN_VALUE;
    int count_;
    double total_;

    public void add(double what) {
      total_ += what;
      if (what < min_)
        min_ = what;
      if (what > max_)
        max_ = what;
      ++count_;
    }
    
    public double min() {
      return min_;
    }
    
    public double max() {
      return max_;
    }
    
    public double avg() {
      return total_/count_;
    }
    
    public int count() {
      return count_;
    }
    
    @Override public String toString() {
      return avg()+" ("+min_+" ... "+max_+")";
    }
  }
  
  
  
  /** Returns the index of the largest value in the array. In case of a tie, an
   * the index is selected randomly.   */
  public static int maxIndex(int[] from, Random rand) {
    int result = 0;
    for (int i = 1; i<from.length; ++i) if (from[i]>from[result]) result = i;
      else if (from[i]==from[result] && rand!=null && rand.nextBoolean()) result = i; // tie breaker
    return result;
  }

  /** Returns the index of the largest value in the array. In case of a tie, an
   * the index is selected randomly.   */
  public static int maxIndex(double[] from, Random rand) {
    int result = 0;
    for (int i = 1; i<from.length; ++i) if (from[i]>from[result]) result = i;
      else if (from[i]==from[result] && rand!=null && rand.nextBoolean()) result = i; // tie breaker
    return result;
  }

  public static int maxIndex(double[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result])
        result = i;
    return result;
  }
  
  public static int maxIndexInt(int[] from, Random rand) {
    int result = 0;
    for (int i = 1; i<from.length; ++i) if (from[i]>from[result]) result = i;
      else if (from[i]==from[result] && rand!=null && rand.nextBoolean()) result = i; // tie breaker
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
  
  public static double lnF(double what) {
    return (what < 1e-06) ? 0 : what * Math.log(what);
  }
  
  public static double entropyOverColumns(double[][] m) {
    double result = 0;
    double total = 0;
    for (int col = 0; col < m[0].length; ++col) {
      double sum = 0;
      for (int row = 0; row < m.length; ++row)
        sum += m[row][col];
      result -= lnF(sum);
      total += sum;
    }
    return (total == 0) ? 0 : (result + lnF(total)) / (total * Math.log(2));
  }
  
  public static double entropyCondOverRows(double[][] m) {
    double result = 0;
    double total = 0;
    for (double[] d : m) {
      double sum = 0;
      for (double dd : d) {
        sum += dd;
        result += lnF(dd);
      }
      result -= lnF(sum);
      total += sum;
    }
    return (total == 0) ? 0 : -result / (total *Math.log(2));
  }
  
  public static String p2d(double d) { return df.format(d); }
  static final DecimalFormat df = new  DecimalFormat ("0.##");
  public static String p5d(double d) { return df5.format(d); }
  static final DecimalFormat df5 = new  DecimalFormat ("0.#####");

  /** Splits the given work into N workers as equally as possible. Returns an
   * array of integers, each per thread that says how much the thread will do.
   * 
   * @param howMuch
   * @param between
   * @return 
   */
  public static int[] splitEquallyBetween(int howMuch, int between) {
    int[] result = new int[between];
    int perOne = howMuch/between;
    int remaining = howMuch - (perOne * between);
    for (int i = 0; i<between; ++i) {
      result[i] = perOne;
      if (remaining>0) {
        result[i] += 1;
        --remaining;
      }
    }
    return result;  
  } 

   public static int set4( byte[] buf, int off, int x ) {
    for( int i=0; i<4; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 4;
  }
  public static int get4( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<4; i++ )
      sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }

  public static int set8d( byte[] buf, int off, double d ) {
    long x = Double.doubleToLongBits(d);
    for( int i=0; i<8; i++ )
      buf[i+off] = (byte)(x>>(i<<3));
    return 8;
  }
  public static double get8d( byte[] buf, int off ) {
    long sum=0;
    for( int i=0; i<8; i++ )
      sum |= ((long)(0xff&buf[off+i]))<<(i<<3);
    return Double.longBitsToDouble(sum);
  }
  
  public static double sum(double[] from) {
    double result = 0;
    for (double d: from)
      result += d;
    return result;
  }
  
}
