package water;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

// Rock-simple Timer
// @author <a href="mailto:cliffc@0xdata.com"></a>
public class Timer {
  public final long _start = System.currentTimeMillis();
  public long time() { return System.currentTimeMillis() - _start; }
  public String toString() {
    final long now = System.currentTimeMillis();
    return toHuman(now - _start, false) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(now)) + ") ";
  }

  public static String toHuman(long msecs, boolean truncate) {
    final long hr  = TimeUnit.MILLISECONDS.toHours  (msecs);  msecs -= TimeUnit.HOURS  .toMillis(hr);
    final long min = TimeUnit.MILLISECONDS.toMinutes(msecs);  msecs -= TimeUnit.MINUTES.toMillis(min);
    final long sec = TimeUnit.MILLISECONDS.toSeconds(msecs);  msecs -= TimeUnit.SECONDS.toMillis(sec);
    final long ms  = TimeUnit.MILLISECONDS.toMillis (msecs);
    if(!truncate || hr != 0) return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms);
    if(min != 0) return String.format("%02d min %02d.%03d sec", min, sec, ms);
    return String.format("%02d.%03d sec", sec, ms);
  }
}
