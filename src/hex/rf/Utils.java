package hex.rf;

import java.text.DecimalFormat;
import java.util.Random;

public class Utils {

  /** Returns the index of the largest value in the array.
   * In case of a tie, an the index is selected randomly.
   */
  public static int maxIndex(int[] from, Random rand) {
    assert rand != null;
    int result = 0;
    int maxCount = 0; // count of maximal element for a 1 item reservoir sample
    for( int i = 1; i < from.length; ++i ) {
      if( from[i] > from[result] ) {
        result = i;
        maxCount = 1;
      } else if( from[i] == from[result] ) {
        if( rand.nextInt(++maxCount) == 0 ) result = i;
      }
    }
    return result;
  }

  public static int maxIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }

  public static double lnF(double what) {
    return (what < 1e-06) ? 0 : what * Math.log(what);
  }

  public static String p2d(double d) { return df.format(d); }
  static final DecimalFormat df = new  DecimalFormat ("0.##");
  public static String p5d(double d) { return df5.format(d); }
  static final DecimalFormat df5 = new  DecimalFormat ("0.#####");


  public static int set4( byte[] buf, int off, int x ) {
    for( int i=0; i<4; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 4;
  }
  public static int get4( byte[] buf, int off ) {
    int sum=0;
    for( int i=0; i<4; i++ ) sum |= (0xff&buf[off+i])<<(i<<3);
    return sum;
  }

  public static int set8d( byte[] buf, int off, double d ) {
    long x = Double.doubleToLongBits(d);
    for( int i=0; i<8; i++ ) buf[i+off] = (byte)(x>>(i<<3));
    return 8;
  }
  public static double get8d( byte[] buf, int off ) {
    long sum=0;
    for( int i=0; i<8; i++ ) sum |= ((long)(0xff&buf[off+i]))<<(i<<3);
    return Double.longBitsToDouble(sum);
  }

  public static int sum(int[] from) {
    int result = 0;
    for (int d: from) result += d;
    return result;
  }

  public static void pln(String s) { System.out.println(s); }
}
