package water;
import java.net.DatagramPacket;

/**
 * A task initiator has his response, we can quit sending him ACKs.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class UDPAckAck extends UDP {
  // Received an ACKACK for a remote Task.  Drop the task tracking
  void call(DatagramPacket pack, H2ONode h2o) {
    int tasknum = get4(pack.getData(),1);
    h2o.remove_task_tracking(tasknum);
    UDPReceiverThread.free_pack(pack);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( long lo, long hi ) {
    int tasknum = (int)((lo>>8)&0xFFFFFFFFL);
    return "task# "+tasknum;
  }
}

