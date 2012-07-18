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
  void call(DatagramPacket pack, H2ONode h2o) {
    int tasknum = get4(pack.getData(),1);
    DFutureTask t = DFutureTask.TASKS.get(tasknum);
    if( t == null )    // Never heard of this task?  Just blow it off.
      return;
    // Do the 2nd half of this task
    t.response(pack);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( long lo, long hi ) {
    int tasknum = (int)((lo>>8)&0xFFFFFFFFL);
    return "task# "+tasknum;
  }
}

