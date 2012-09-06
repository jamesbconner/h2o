/**
 * 
 */
package water;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

import water.web.ProgressReport;

/**
 * Log hub is a central part for collecting stdout/stderr of local and remote nodes.
 * Log hub implements publish-subscribe pattern. 
 * 
 * <p>Publishers are threads producing stdout
 * and stderr. The streams are collected by {@link LogWrapper} which encapsulates {@link Log}).
 * The {@link LogWrapper} split streams into events which are sent to {@link LogHub} which resends
 * them to subscribed listeners.</p>
 * 
 * 
 * <p>There are two kinds of subscribers - local and remote.   
 * The local subscriber {@link LocalLogSubscriber} collects all events and publishes it via {@link InputStream} which is published via
 * REST API (see {@link ProgressReport}). If the local subscriber is registered, all nodes in the cloud are asked 
 * for sending their stdout/stderr. The remote subscriber {@link RemoteLog} sends all events via UDP to all registered
 * nodes.</p> 
 * 
 * @author michal
 *
 */
public class LogHub extends Thread {
  
  // Log event kind.
  enum LogKind {
    LOCAL_STDOUT  ("out"),
    LOCAL_STDERR  ("err"),
    REMOTE_STDOUT ("out"),
    REMOTE_STDERR ("err");
    
    String label;
    
    private LogKind(String label) { this.label = label; }
  }
  
  // Log event holder class.
  public static final class LogEvent {
    byte[] data;
    String threadName;
    H2ONode node;
    LogKind kind;
    
    public void reset() { data = null; threadName = null; node = null;}
  }    
  
  @Override
  public void run() {
    try {
      while (true) { 
        LogEvent e = eventsBuffer.take(); // calling take method will wait for an item
        
        // Propagate event to subscribers:      
        synchronized( this ) {
          Iterator<LogSubscriber> it = subscribers.iterator();
          while (it.hasNext()) {
            LogSubscriber s = it.next();
            // check if the reader is alive
            if (s.isAlive()) {
              if (s.accept(e)) {
                s.write(e);
              }
            } else {      
              // subscriber is dead => remove it and unsubscribe
              it.remove();
              unsubscribeListener(s, false);
            }
          }
        }
        // Free the event.
        LogHub.free_event(e);
      } 
    } catch (InterruptedException ie) { /* swallow the ie */ }
  }
  
  // Start the LogHub thread
  public static void prepare_log_hub() {    
    INSTANCE.start();    
  }  
  
  static final LogHub INSTANCE = new LogHub();    
  private LogHub() {}
 
  /* Max size of log event buffer. If there is not enough space in the buffer, the events are dropped. */
  static final int EVENTS_BUFFER_SIZE = 128; 
  
  private ArrayBlockingQueue<LogEvent> eventsBuffer = new ArrayBlockingQueue<LogHub.LogEvent>(EVENTS_BUFFER_SIZE);
  private LinkedList<LogSubscriber> subscribers = new LinkedList<LogSubscriber>(); 
  
  protected void write(final LogEvent logEvent) {
    // Offer does not block, however in this case it losses data. 
    if (!eventsBuffer.offer(logEvent)) {// if eventsBuffer is full => the event is dropped but it needs to be freed. 
      LogHub.free_event(logEvent);
    }
  }   
  
  protected void subscribeListener(final LogSubscriber s) {
    //System.err.println("Subscribing: " + s);
    synchronized( this ) { if (!subscribers.contains(s)) subscribers.add(s);  }
    // if the subscriber is local => ask other nodes to send their stdout/stderr
    if (s.isLocal()) { RemoteLog.enable_remote_logging(); }
  }
  
  protected void unsubscribeListener(final LogSubscriber s, boolean remove) {
    //System.err.println("Unsubscribing: " + s);
    if (remove) { 
      synchronized( this ) { subscribers.remove(s);  } 
    }
    if (s.isLocal()) { RemoteLog.disable_remote_logging(); }
  }  
    
  public static void write(final byte[] data, final String threadName, final H2ONode node, final LogKind kind) {
    LogEvent e = LogHub.get_event();
    e.data = data;
    e.threadName = threadName;
    e.node = node;
    e.kind = kind;
    INSTANCE.write(e);                
  }
    
  public static void subscribe(final LogSubscriber s) {
    INSTANCE.subscribeListener(s);
  }
  
  public static void unsubscribe(final LogSubscriber s) {        
    INSTANCE.unsubscribeListener(s, true);    
  }
  
  /**
   * This is log hub subscriber interface.
   */
  public static interface LogSubscriber {
    /** Notify about output produced by thread with threadName running on the node. */ 
    void write(LogEvent event);
        
    /** Identifies local subscriber */
    boolean isLocal();
    
    /** Check if the subscriber is alive */
    boolean isAlive();
    
    /** Check if the subscriber accept the log event. */
    boolean accept(LogEvent event);
  }  
  
  /** Simple slab allocator for LogEvent type 
   * - it is based on the same allocator in {@link UDPReceiverThread} 
   */
  private static ArrayList<LogEvent> FREE_EVENTS_LIST = new ArrayList<LogEvent>();
  
  public static void free_event(LogEvent event) {
    event.reset();
    synchronized( LogHub.class ) { FREE_EVENTS_LIST.add(event); }
  }
  
  public static LogEvent get_event() {
    LogEvent e = null;
    synchronized( LogHub.class ) {
      int size = FREE_EVENTS_LIST.size();
      if (size > 0) e = FREE_EVENTS_LIST.remove(size-1);
    }    
    
    // No free event on the list => we need to create a new one
    e = new LogEvent();
    
    return e;
  }
}
