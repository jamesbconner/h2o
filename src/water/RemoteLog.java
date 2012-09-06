/**
 * 
 */
package water;

import java.net.DatagramPacket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

import water.LogHub.LogEvent;
import water.LogHub.LogKind;
import water.LogHub.LogSubscriber;

/**
 * UDP packet representing remote log.
 * 
 * @author michal
 *
 */
public class RemoteLog extends UDP implements LogSubscriber {
  
  // All subscribed nodes for this node.
  public static final HashSet<H2ONode> subscribedNodes = new HashSet<H2ONode>(); 
  
  @Override
  void call(DatagramPacket pack, H2ONode target) {
    // Ignore this node packets.
    if (target == H2O.SELF)
      return;    
    // Get log command. 
    byte cmd = pack.getData()[CMD_OFF];    
    switch (cmd) {
    
    // receive request to start publishing stdout/stderr
    case 1:      
      subscribedNodes.add(target);
      LogHub.subscribe(this);
      break;
    
    // receive request to stop producing stdout/stderr  
    case 0:
      subscribedNodes.remove(target);
      // if there is no subscribers => unsubscribe the this log subscriber 
      if (subscribedNodes.isEmpty()) LogHub.unsubscribe(this);
      break;

    // received log event => create LogEvent and send it to LogHub  
    case 2:
      // the code should check if the node is subscribed to receive log events 
      write_logevent(pack.getData(), target);      
      break;
    }            
    // it is stateless packet, i do not need it anymore => free it
    UDPReceiverThread.free_pack(pack);                
  }
  
  // Counter how many times function enable_remote_logging was called. 
  // It can be called from different threads in parallel => use AtomicInteger  
  static final AtomicInteger counterEnableLoggingCalls = new AtomicInteger(0); 
  /**
   * Ask all nodes in the cloud to send their stdout &amp; stderr.
   * Compute how many times was called to properly stop logging if multiple local 
   * readers have called enable_remote_logging.
   */  
  static public void enable_remote_logging() {
    byte[] buf = new byte[16];
    set_ctrl(buf,UDP.udp.log.ordinal());
    buf[CMD_OFF] = 1; // 1 = enable
    // send to all nodes in the cloud - it can be send multiple times.
    MultiCast.multicast(buf);
    counterEnableLoggingCalls.incrementAndGet();
  }
  
  /**
   * Ask all nodes in the current cloud to stop sending their stdout/stderr to this node.
   * The method safely handle case when multiple local readers request remove logs.
   */
  static public void disable_remote_logging() {
    if (counterEnableLoggingCalls.decrementAndGet() == 0) {
      byte[] buf = new byte[16];
      set_ctrl(buf, UDP.udp.log.ordinal());
      buf[CMD_OFF] = 0; // 0 = disable
      // send to all nodes in the cloud to stop sending their stdout&stderr
      MultiCast.multicast(buf);                
    }
  }
  
  // Offsets to referring UDP packet body
  static final int CMD_OFF = SZ_PORT;                       // 1 byte:  log command 0=disable logging, 1=enable logging, 2=log message 
  static final int DATA_SIZE_OFF = CMD_OFF + 1;             // 2 bytes: size of log message 
  static final int THREADNAME_SIZE_OFF = DATA_SIZE_OFF + 2; // 1 byte:  size of thread name < 255 
  static final int KIND_OFF = THREADNAME_SIZE_OFF + 1;      // 1 byte:  size of log message
  static final int DATA_OFF = KIND_OFF + 1;                 // x bytes: log message data 
  
  static public void send_log_event(LogEvent e) {
    byte[] threadname = e.threadName.getBytes();
    //  size: transfer dat + log command + data size + name size + kind  + data + threadName 
    int size = SZ_PORT + 1 + 2 + 1 + 1 + e.data.length + threadname.length ; // +1 = kind
    assert size < 1600;                    // the size cannot exceed size of UDP packet
    assert threadname.length < 255;        // the length of thread's name cannot exceed size of 1 byte
    byte[] buf = new byte[size];
    set_ctrl(buf, UDP.udp.log.ordinal());  // setup control bit
    buf[CMD_OFF]             = 2;          // setup log command: 2 = transfer of log event
    set2(buf, DATA_SIZE_OFF, e.data.length);             // write length of data
    buf[THREADNAME_SIZE_OFF] = (byte) threadname.length; // write length of thread's name 
    buf[KIND_OFF]            = (byte) e.kind.ordinal();  // write kind of event
    
    System.arraycopy(e.data, 0, buf, DATA_OFF, e.data.length); // write data    
    int threadNameOff = DATA_OFF + e.data.length;              // write thread's name 
    System.arraycopy(threadname, 0, buf, threadNameOff, threadname.length);
    
    // Send the packet only to registered subscribers.
    MultiCast.singlecast(subscribedNodes.toArray(new H2ONode[subscribedNodes.size()]), buf);
  }
  
  static public void write_logevent(final byte[] buf, final H2ONode from) {
    int  dataSize       = get2(buf, DATA_SIZE_OFF); 
    byte threadNameSize = buf[THREADNAME_SIZE_OFF]; 
    int  threadNameOff  = DATA_OFF + dataSize;  
    byte kind           = buf[KIND_OFF]; assert kind >= 0 && kind < LogKind.values().length; 
    
    byte[] data = Arrays.copyOfRange(buf, DATA_OFF, DATA_OFF + dataSize); 
    byte[] threadname = Arrays.copyOfRange(buf, threadNameOff, threadNameOff + threadNameSize); 
    
    LogHub.write(data, new String(threadname), from, LogKind.values()[kind]);    
  }

  // Distribute log event through the network.
  @Override public void write(LogEvent event) {
    if (event.kind == LogKind.LOCAL_STDOUT) event.kind = LogKind.REMOTE_STDOUT;
    if (event.kind == LogKind.LOCAL_STDERR) event.kind = LogKind.REMOTE_STDERR;
    RemoteLog.send_log_event(event);  
  }
  @Override public boolean isLocal() { return false; }
  @Override public boolean isAlive() { return true; }
  @Override public boolean accept(final LogEvent e) { return e.kind == LogKind.LOCAL_STDOUT || e.kind == LogKind.LOCAL_STDERR; } // accept only local events
}

