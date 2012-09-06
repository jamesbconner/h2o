package water;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import water.LogHub.LogKind;

/**
 * Log wrapper encapsulates stdout/stderr streams (see {@link Log#hook_sys_out_err()}) 
 * and split the streams into events (event correspond to a line). The events are sent 
 * to {@link LogHub} which resends them to its subscribers.
 * 
 * @author michal
 *
 */
public class LogWrapper extends PrintStream {

  static final ThreadLocal<ByteArrayOutputStream> OUT = new ThreadLocal<ByteArrayOutputStream>() {
    protected ByteArrayOutputStream initialValue() {
      return new ByteArrayOutputStream();
    }
  };
  
  private LogKind logKind;
  
  public LogWrapper(PrintStream logger, LogKind kind) {
    super(logger);
    this.logKind = kind;
  }

  @Override
  public void write(byte[] buf, int off, int len) {    
    ByteArrayOutputStream baos = OUT.get();
    int lastNewlineOff = off;
    int newLineOff = -1;
    for( int i = off; i < off + len; i++ ) {
      if( buf[i] == '\n' ) { // NOTE: perhaps it is possible to merge the lines into one event
        newLineOff = i;       
        // compose the rest of line
        baos.write(buf, lastNewlineOff, newLineOff - lastNewlineOff);
        // send line to LogHub
        LogHub.write(baos.toByteArray(), Thread.currentThread().getName(), H2O.SELF, logKind);
                                
        lastNewlineOff = newLineOff + 1;                                
        baos.reset();
      }
    } 
    // no '\n' found => record buffer
    if (newLineOff == -1) {
      baos.write(buf, off, len);
    }

    // Redirect to logger.   
    super.write(buf, off, len);
  }
}
