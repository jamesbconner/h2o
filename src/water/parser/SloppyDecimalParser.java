
package water.parser;

/* A parser for signed decimal values of the form  "-4.5" or "11.23", it does not try to
 * be a complete parser for Doubles. Anything that strays from the simple format will c
 * cause it to return Double.NaN.
 * Profiling suggested that using the Java Double parser is expensive and does result in
 * quite a lot of allocation. */
public class SloppyDecimalParser {

  private static final int ERROR    = -1;
  private static final int LEADING  = 0;
  private static final int BODY     = 1;
  private static final int TRAILING = 2;
  private static final int NEGATE   = 3;
  private static final int LOWER    = 4;

  private int _state = LEADING;
  private double _value, _neg=1;
  private double _pos;

  public void reset() { _state = LEADING;  _value = 0; _neg = 1;}

  @SuppressWarnings("fallthrough")
  public void addCharacter(byte b) {
    char ch = (char) b;
    switch( _state ) {
    case ERROR: return;
    case TRAILING:
      if( Character.isWhitespace(ch) ) return;
      _state = ERROR; return;
    case LEADING:
      if( Character.isWhitespace(ch) ) return;
      if ( ch == '-' ) { _state = NEGATE; _neg = -1; return; }
      _state = BODY;
    case LOWER:
      if( Character.isWhitespace(ch) ) { _state = TRAILING;  return;  }
      if( Character.isDigit(ch) ) break;
      _state = ERROR; return;
    case NEGATE:
      if( Character.isWhitespace(ch) ) return;
      _state = BODY;
    case BODY: // fall-through
      if( Character.isWhitespace(ch) ) { _state = TRAILING;  return;  }
      if( Character.isDigit(ch) ) break;
      if( ch == '.') { _state = LOWER;  _pos = 1; return; }
      _state = ERROR; return;
    }
    int v = ch - '0';
    if (_state == BODY) {
      _value = (_value*10) + v;
    } else { // STATE == LOWER
      _pos *= 10;
      _value +=  v / _pos;
    }
  }

  public double doubleValue() {
    if( _state == ERROR ) return Double.NaN;
    if( _state == BODY || _state == LOWER|| _state == TRAILING ) return _value * _neg;
    else return Double.NaN;
  }
}
