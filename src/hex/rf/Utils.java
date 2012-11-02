
package hex.rf;

import java.sql.Date;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Utils {


  /** Returns the index of the largest value in the array. In case of a tie, an
   * the index is selected randomly.   */
  public static int maxIndex(int[] from, Random rand) {
    int result = 0;
    for (int i = 1; i<from.length; ++i) if (from[i]>from[result]) result = i;
      else if (from[i]==from[result] && rand!=null && rand.nextBoolean()) result = i; // tie breaker
    return result;
  }

  public static int maxIndex(int[] from) {
    int result = 0;
    for (int i = 1; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }


  public static String join(int[] what, String with) {
    if (what==null)  return "";
    StringBuilder sb = new StringBuilder();
    sb.append(what[0]);
    for (int i = 1; i<what.length;++i) sb.append(with+what[i]);
    return sb.toString();
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

  private static ConcurrentHashMap<String,Long> startTimers = new ConcurrentHashMap<String, Long>();
  private static ConcurrentHashMap<String,Long> endTimers = new ConcurrentHashMap<String, Long>();

  public static void clearTimers() {
    startTimers = new ConcurrentHashMap<String, Long>();
    endTimers = new ConcurrentHashMap<String, Long>();
  }

  public static void startTimer(String name) {
    if( startTimers.putIfAbsent(name, System.currentTimeMillis()) != null )
      pln("[RF] Trying to start timer " + name +" twice");
  }

  public static String printTimer(String name) {
    long now = System.currentTimeMillis();
    Long old = startTimers.get(name);
    if( old==null ) return "[RF] Trying to print timer " + name +" before start.";
    Long L = endTimers.get(name);
    long l = L==null? now - old.longValue() : L.longValue();
    if (L == null) endTimers.put(name, l);

    final long hr = TimeUnit.MILLISECONDS.toHours(l);
    final long min = TimeUnit.MILLISECONDS.toMinutes(l - TimeUnit.HOURS.toMillis(hr));
    final long sec = TimeUnit.MILLISECONDS.toSeconds(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min));
    final long ms = TimeUnit.MILLISECONDS.toMillis(l - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min) - TimeUnit.SECONDS.toMillis(sec));
    return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(System.currentTimeMillis())) + ") ";
  }
}
