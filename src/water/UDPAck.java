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
    byte[] buf = pack.getData();
    int tasknum = get_task(buf);
    DFutureTask<?> t = DFutureTask.TASKS.get(tasknum);
    if( t != null )             // Do the 2nd half of this task
      t.response(pack);
    // ACKACK the remote, telling him "we got the answer"
    set_ctrl(buf,udp.ackack.ordinal());
    clr_port(buf); // Re-using UDP packet, so side-step the port reset assert
    MultiCast.singlecast(h2o,buf,UDP.SZ_TASK);
    UDPReceiverThread.free_pack(pack);
  }

  // Pretty-print bytes 1-15; byte 0 is the udp_type enum
  public String print16( byte[] buf ) {
    int tasknum = get_task(buf);
    return "task# "+tasknum;
  }
}

