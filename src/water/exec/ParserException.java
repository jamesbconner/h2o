
package water.exec;

import water.exec.RLikeParser.Token;

/**
 *
 * @author peta
 */
public class ParserException extends Exception {
  public ParserException(String msg) {
    super(msg);
  }
  
  public ParserException(Token.Type expected, Token.Type found) {
    super("Expected token "+expected.toString()+", but "+found.toString()+" found");
  }
  
  public ParserException(String expected, Token.Type found) {
    super("Expected "+expected+", but "+found.toString()+" found");
  }
  
}
