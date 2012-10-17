package water.parser;

/**
 * Simple string parser only records characters composing fields.
 * Provides the same API as {@link DecimalParser}.
 *
 * @author michal
 * @see DecimalParser
 */
public class TextualParser {

  private static final int LEADING  = 0;
  private static final int BODY     = 1;
  private static final int ESCAPED_BODY     = 2;

  private final boolean _trimWS; // Trim the resulting string.
  private final StringBuilder _buffer;

  private int _state;

  public TextualParser() {
    this(true);
  }

  public TextualParser(boolean trimWS) {
    _trimWS = trimWS;
    _buffer = new StringBuilder(64);

    _state  = LEADING;
  }

  public void addCharacter(byte b) {
    char ch = (char) b;
    switch (_state) {
    case LEADING:
      if (ch == ' ' && _trimWS) break;
      if (ch == '"' )  {
        _state = ESCAPED_BODY;
      }
      else {
        _buffer.append(ch);
        _state = BODY;
      }
      break;

    case BODY:
      if (ch == '"')
        _state = ESCAPED_BODY;
      else
        _buffer.append(ch);
      break;

    case ESCAPED_BODY:
      if (ch == '"')
        _state = BODY;
      else
        _buffer.append(ch);
      break;
    }
  }

  public void reset() {
    _buffer.setLength(0);
    _state = LEADING;
  }

  /**
   * Returns a string value for given row.
   * The value is trimmed if required.
   */
  public String stringValue() {
    int end = _buffer.length()-1;
    if (_trimWS) {
      while (end > 0 && _buffer.charAt(end) == ' ') end--;
    }
    return _buffer.substring(0, end+1);
  }
}
