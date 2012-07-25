package water;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import jsr166y.*;

/**
 * A class to handle the work of a received UDP packet.  Typically we'll do a
 * small amount of work based on the packet contents (such as returning a Value
 * requested by another Node, or recording a heartbeat).
 * 
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */

public class FJPacket extends CountedCompleter {

  // The packet in question.  Note: these are somewhat large (includes a
  // MTU-sized byte-buffer) and are Object-Pooled at the moment.
  public final DatagramPacket _pack;
  // The interned H2O struct obtained from the packets IP address
  public final H2ONode _h2o;

  FJPacket( DatagramPacket p, H2ONode sender ) { _pack = p; _h2o = sender; }

  // Main computation task!  Called by some worker from the FJ pool
  public void compute() {
    // Switch to a handler based on the first byte of the packet
    int first_byte = UDP.get_ctrl(_pack.getData());
    UDP.udp.UDPS[first_byte]._udp.call(_pack,_h2o);
    // Complete this task
    tryComplete();
  }

  // Oops, uncaught exception
  public boolean onExceptionalCompletion( Throwable ex, CountedCompleter caller ) {
    ex.printStackTrace();
    return true;
  }

  // Same dispatch, but for TCP args.
  // Dispatch on the enum opcode and Do The Call
  static public void call( int first_byte, DataInputStream dis, H2ONode h2o ) {
    try {
      UDP.udp.UDPS[first_byte]._udp.tcp_read_call(dis,h2o);
    } catch(IOException e) {
      System.err.println("Call failed: " +e);
    } catch(ArrayIndexOutOfBoundsException e) {
      System.err.println("TCP Format Error: " +e);
    }
  }
}
