package water;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Stack;

/**
* Class for managing System.out & System.err. All normal output is pumped
* through some thread-local variables which choose to send the output on to
* the normal Sys.out/err or buffer it, on a per-thread basis.
* @author jan
* @author cliffc
*
*/
public final class Log {

  // Print to the original STDERR & die
  public static void die( String s ) {
    System.err.println(s);
    System.exit(-1);
  }

}

