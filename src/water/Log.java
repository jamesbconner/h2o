package water;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Class for managing System.out & System.err.  All normal output is pumped
 * through some thread-local variables which choose to send the output on to
 * the normal Sys.out/err or buffer it, on a per-thread basis.
 * @author jan
 * @author cliffc
 * @author michal
 *
 */
public final class Log extends PrintStream {

  // Make thread-local buffering for peoples who want to go there.  Allows Log
  // output to be buffered and redirected.  By default output is shipped to the
  // "normal place" of the original System.out/.err.
  static final ThreadLocal<PrintStream> OUT =
    new ThreadLocal<PrintStream>() {
    protected PrintStream initialValue() {
      return H2O.OUT;
    }
  };
  static final ThreadLocal<PrintStream> ERR =
    new ThreadLocal<PrintStream>() {
    protected PrintStream initialValue() {
      return H2O.ERR;
    }
  };

  // ByteArrayOutputStream, as redirected on demand.  This allows System.out &
  // System.err to be buffered & redirected on a per-thread basis.
  static final ThreadLocal<Stack<ByteArrayOutputStream>> BOS =
    new ThreadLocal<Stack<ByteArrayOutputStream>>() {
    protected Stack<ByteArrayOutputStream> initialValue() {
      return new Stack<ByteArrayOutputStream>();
    }
  };

  final boolean _is_out;        // true for Sys.out, false for Sys.err

  private Log( boolean is_out ) { super( is_out ? H2O.OUT : H2O.ERR ); _is_out = is_out;  }
  public static void hook_sys_out_err() {
    System.setOut(new Log(true ));
    System.setErr(new Log(false));
  }
  
  // append node/thread info to each line sent to output
  @Override
  public void println(String x) {
    StringBuilder sb = new StringBuilder("[");
    sb.append(H2O.SELF); sb.append(']');
    sb.append('['); sb.append(Thread.currentThread().getName()); sb.append("]: ");
    sb.append(x);
    
    super.println(sb);    
  }

  // Redirect writes through the thread-local variables
  public void write(byte buf[], int off, int len) {
    // notify listeners sinks
    if (_is_out) // FIXME
      notifyListeners(buf, off, len);
    // write to original output
    (_is_out ? OUT : ERR).get().write(buf,off,len);
  }

  // Print to the original STDERR & die
  public static void die( String s ) {
    H2O.ERR.println(s);
    System.exit(-1);
  }

  // --------------------------------------------------------------------------
  // Thread-local buffering of system out/err
  public static void buffer_sys_out_err() {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    BOS.get().push(bos);
    PrintStream ps = new PrintStream(bos);
    OUT.set(ps);
    ERR.set(ps);
  }
  public static ByteArrayOutputStream unbuffer_sys_out_err() {
    Stack<ByteArrayOutputStream> stk_bos = BOS.get();
    ByteArrayOutputStream bos_old = stk_bos.pop();

    if( stk_bos.empty() ) {
      OUT.set(H2O.OUT);         // Reset back to unbuffered land
      ERR.set(H2O.ERR);
    } else {
      ByteArrayOutputStream bos_cur = stk_bos.peek();
      PrintStream ps = new PrintStream(bos_cur);
      OUT.set(ps);
      ERR.set(ps);
    }
    return bos_old;
  }
  
  
  // care about concurrency 
  List<LogListener> listeners = new ArrayList<LogListener>();
  
  // 
  private void notifyListeners(byte[] buf, int off, int len) {
    for (LogListener listener : listeners) {
      listener.notify(H2O.SELF, Thread.currentThread().getName(), buf, off, len);      
    }
  }
  
  /**
   * This log listener.
   */
  public static interface LogListener {
    /** Notify about output produced by thread with threadName running on the node. */ 
    void notify(H2ONode node, String threadName, byte[] buf, int off, int len);
    
    InputStream getInputStream();
  }
  
  public static class LocalLogListenerImpl implements LogListener {
    private LogPipe pipe = new LogPipe();

    @Override public void notify(H2ONode node, String threadName, byte[] buf, int off, int len) {
      try {
        pipe.getSink().write(buf, off, len);
      } catch( IOException e ) {
        // TODO unregister itself from 
      }
    }

    @Override
    public InputStream getInputStream() {
      return pipe.getSource();      
    }
  }
  
  /**
   * Register listener to this log.
   * @return the registered listener
   */
  public LogListener registerListener(final LogListener listener) {
    listeners.add(listener);
        
    return listener;
  }
  
  public LogListener registerListener() {
    return registerListener(new LocalLogListenerImpl());
  }
  
  public void unregisterListener(final LogListener listener) {
    listeners.remove(listener);
  }
    
  /**
   * Pipe between threads - multiple writers - single reader (REST API)
   * 
   * Note: I do not want to use java.nio.Pipe due to it does not specify explicit
   * blocking policy. I do not want to block writers - the implementation prefers to lost data.
   */
  public static class LogPipe {
    
    public static final int INITIAL_SIZE = 512;
    public static final int MAX_SIZE = 32768;
        
    private byte[] buffer;
    private boolean availableData = false;
    private int writePosition = 0;
    private int countToBeRead = 0;
    private int readPosition  = -1;
    
    private Sink sink = new Sink();
    private Source source = new Source();
    
    public Sink getSink()     { return sink; };
    public Source getSource() { return source; };
    
    public LogPipe() { this(INITIAL_SIZE); }    
    public LogPipe(int size) { buffer = new byte[size]; }
    
    // Write to the buffer supports multiple writers
    // Buffer expands to its maximal size, then the round buffer is used.
    protected void write(int b) {
      synchronized( buffer ) {        
        // do not overwrite reader
        if (writePosition == buffer.length && buffer.length < MAX_SIZE) { // buffer can be resized
            buffer = Arrays.copyOf(buffer, Math.max(buffer.length << 1, writePosition));
        } 
        
        buffer[writePosition++] = (byte) b;
        countToBeRead++;
        
        if (!availableData) {
          availableData = true;
          buffer.notify();
        }
      }
    }
    
    protected int read() throws IOException {
      // read buffer is empty or consumer already read all data
      if (readPosition == -1 || readPosition == writePosition) {
        availableData = false;
        // this call will block if there is no data in write buffer to transfer to read buffer
        fillReadBuffer();
      }     
      
      //System.err.println(new String(buffer, readPosition, writePosition-1));
      int result = buffer[readPosition++];            
      countToBeRead--;      
      return result;
    }
    
    private void fillReadBuffer() {
      synchronized( buffer ) {
        if (!availableData) {
          readPosition = 0;
          writePosition = 0;
        }
        while (!availableData) {
          try {
            buffer.wait(1000); // wait for data
          } catch( InterruptedException e ) {}
        }        
      }
    }
    
    protected int available() {
      return (countToBeRead > 0 ? countToBeRead : 1); // prevent against inputstream close
    }
    
    public class Sink extends OutputStream {
      @Override public void write(int b) throws IOException {
        LogPipe.this.write(b);
      }
    }
    
    public class Source extends InputStream {

      @Override public int read() throws IOException {
        return LogPipe.this.read();
      } 
      
      @Override public int available() throws IOException {
        return LogPipe.this.available();
      }

      @Override public void close() throws IOException {
        System.err.println("Log.LogPipe.Source.close()");
      }
    }
  }
}
