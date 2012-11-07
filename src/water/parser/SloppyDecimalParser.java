package water.parser;

/* A parser for signed decimal values of the form "-4.5" or "11.23", it does
 * not try to be a complete parser for Doubles.  Anything that strays from the
 * simple format will cause it to return Double.NaN.
 * Profiling suggested that using the Java Double parser is expensive and does
 * result in quite a lot of allocation. */
public class SloppyDecimalParser {

  private static final int ERROR    = -1;
  private static final int LEADING  = 0;
  private static final int BODY     = 1;
  private static final int TRAILING = 2;
  private static final int NEGATE   = 3;
  private static final int LOWER    = 4;

  private int _state = LEADING;
  private long _value;
  private boolean _neg;
  private double _scale = 1.0;
  private int _digs = 0;        // total digits

  public void reset() { _state = LEADING;  _value = 0; _neg = false; _scale = 1.0; _digs = 0;}

  @SuppressWarnings("fallthrough")
  public void addCharacter(byte b) {
    char ch = (char) b;
    switch( _state ) {
    case TRAILING:
      if( !Character.isWhitespace(ch) ) _state = ERROR;
    case ERROR: return;
    case LEADING:
      if( Character.isWhitespace(ch) ) return;
      _state = BODY;
      if( ch == '-' ) { _neg = true; return; }
    case BODY: // fall-through
      if( Character.isDigit(ch) ) break;
      _state = Character.isWhitespace(ch) ? TRAILING : (ch == '.' ? LOWER : ERROR);
      return;
    case LOWER:
      if( Character.isDigit(ch) ) { _scale *= .1; break; }
      _state = Character.isWhitespace(ch) ? TRAILING : ERROR;
      return;
    }
    _value = _value*10 + (ch-'0');
    _digs++;
  }

  public double doubleValue() {
    if( _state == ERROR || _state == LEADING || _state == NEGATE ) return Double.NaN;
    if( _neg ) _value = -_value;
    double d = _value*_scale;
    // Values with 0-3 digits we'll represent as scaled decimals.
    // Values with 4-8 digits we'll represent as floats.
    // Knock low-precision decimal values back to float precision
    if( 3 < _digs && _digs <= 8 ) d = (float)d;
    return d;
  }
}
