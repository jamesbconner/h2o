package water;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Stack;

import water.LogHub.LogKind;

/**
 * Class for managing System.out & System.err.  All normal output is pumped
 * through some thread-local variables which choose to send the output on to
 * the normal Sys.out/err or buffer it, on a per-thread basis.
 * @author jan
 * @author cliffc
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
    System.setOut(new LogWrapper(new Log(true), LogKind.LOCAL_STDOUT));
    System.setErr(new LogWrapper(new Log(false), LogKind.LOCAL_STDERR));
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
}
