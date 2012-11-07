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
    long l = now-_start;
    final long hr  = TimeUnit.MILLISECONDS.toHours  (l);  l -= TimeUnit.HOURS  .toMillis(hr);
    final long min = TimeUnit.MILLISECONDS.toMinutes(l);  l -= TimeUnit.MINUTES.toMillis(min);
    final long sec = TimeUnit.MILLISECONDS.toSeconds(l);  l -= TimeUnit.SECONDS.toMillis(sec);
    final long ms  = TimeUnit.MILLISECONDS.toMillis (l);
    return String.format("%02d:%02d:%02d.%03d", hr, min, sec, ms) + " (Wall clock time: " +
        new SimpleDateFormat("dd-MMM hh:mm").format(new Date(now)) + ") ";
  }
}
