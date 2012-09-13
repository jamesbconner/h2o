package water.parser;

/**
 * Simple string parser only records characters composing fields.
 * Provides the same API as {@link DecimalParser}.
 * 
 * NOTE: we can implement here handling leading/trailing whitespaces.  
 * 
 * @author michal
 * @see DecimalParser
 */
public class TextualParser {
  
  private StringBuilder _buffer;
  
  public TextualParser() {    
    _buffer = new StringBuilder(64);
  }
  
  public void addCharacter(byte b) {
    char ch = (char) b;   
    _buffer.append(ch);
  }
  
  public void reset() {
    _buffer.setLength(0);
  }
  
  public String stringValue() {
    return _buffer.toString();
  }
}
