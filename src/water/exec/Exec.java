package water.exec;
import water.*;

/**
 * Execute a generic R string, in the context of an H2O Cloud
 *
 * @author cliffc@0xdata.com
 */
public class Exec {
  // Execute some generic R string.  Return a
  public static Key exec( String x ) {
    Expr e = new RLikeParser().parse(x);
    Key k = e.eval(Key.make("Result"));
    return k;
  }
}
