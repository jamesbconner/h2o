package water;
import java.net.DatagramPacket;

/**
 * A remote task request has just returned an ACK with answer
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPAck extends UDP {
  // Received an ACK for a remote Task.  Ping the task.
  AutoBuffer call(AutoBuffer ab) {
    RPC<?> t = RPC.TASKS.get(ab.getTask());
    assert t== null || t._tasknum == ab.getTask();
    return t == null ? ab       // Never heard of this task?  Just blow it off.
      : t.response(ab);         // Do the 2nd half of this task
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( AutoBuffer b ) {
    int tasknum = b.getTask();
    return "task# "+tasknum;
  }
}

