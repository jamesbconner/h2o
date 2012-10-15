
package water.exec;

import water.exec.RLikeParser.Token;

/**
 *
 * @author peta
 */
public class ParserException extends Exception {
  public final int _pos;
  public ParserException(int pos, String msg) {
    super(msg);
    _pos = pos;
  }
  
  public ParserException(int pos, Token.Type expected, Token.Type found) {
    super("Expected token "+expected.toString()+", but "+found.toString()+" found");
    _pos = pos;
  }
  
  public ParserException(int pos, String expected, Token.Type found) {
    super("Expected "+expected+", but "+found.toString()+" found");
    _pos = pos;
  }
  
}
