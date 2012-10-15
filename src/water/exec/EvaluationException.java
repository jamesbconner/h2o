
package water.exec;

/**
 *
 * @author peta
 */
public class EvaluationException extends Exception {
  public final int _pos;
  public EvaluationException(int pos, String msg) {
    super(msg);
    _pos = pos;
  }
}
