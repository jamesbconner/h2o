package water.parser;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import com.google.common.base.Throwables;

public class DecimalParser {
  private static final Class<String> CLAZZ;
  private static final Constructor<String> CTOR;
  private static final Field COUNT;
  private static final Field VALUE;
  static {
    try {
      CLAZZ = String.class;
      CTOR = CLAZZ.getDeclaredConstructor(int.class, int.class, char[].class);
      CTOR.setAccessible(true);
      Field f = null;
      try {
        f = CLAZZ.getDeclaredField("count");
        f.setAccessible(true);
      } catch (NoSuchFieldException nfe) { f = null; }
      COUNT = f;
      
      try {
        f = CLAZZ.getDeclaredField("value");
        f.setAccessible(true);
      } catch (NoSuchFieldException nfe) { f = null; }
      VALUE = f;
      
    } catch (Exception e) { throw Throwables.propagate(e); }
  }
  
  // We do a poor man's parsing of valid number/not valid number
  // to fail fast for things that really won't parse
  private static final int ERROR    = -1;
  private static final int LEADING  = 0;
  private static final int BODY     = 1;
  private static final int TRAILING = 2;
  
  private final String _str;
  private int _len;
  private int _state;
  private final char[]   _digits;
  private final char[][] _digitLens;
  public DecimalParser() {
    try {
      _len = 0;
      _state = LEADING;
      _digits = new char[64];
      if( COUNT != null ) {
        _digitLens = null;
        _str = CTOR.newInstance(0, 0, _digits);
      } else {
        assert VALUE != null;
        _digitLens = new char[64][];
        _str = new String();
        for( int i = 0; i < 64; ++i ) {
          _digitLens[i] = new char[i+1];
        }
      }
    } catch( Exception e ) { throw Throwables.propagate(e); }
  }
  
  public void reset() {
    _len = 0;
    _state = LEADING;
  }
  
  public void addCharacter(byte b) {
    char ch = (char) b;
    switch( _state ) {
    case ERROR: return;
    case TRAILING:
      if( Character.isWhitespace(ch) ) return;
      _state = ERROR;
      return;
    case LEADING:
      if( Character.isWhitespace(ch) ) return;
      _state = BODY;
    case BODY:
      if( Character.isWhitespace(ch) ) {
        _state = TRAILING;
        break;
      }
      if( Character.isDigit(ch) ) break;
      switch( ch ) {
      case '.':
      case 'e':
      case 'E':
      case '-':
        break;
      default:
        _state = ERROR;
        return;
      }
    }
    if( _len < _digits.length ) {
      _digits[_len++] = ch;
    } else {
      _state = ERROR;
    }
  }
  
  public double doubleValue() {
    if( _state == ERROR ) return Double.NaN;
    try {
      if( COUNT != null ) {
        COUNT.setInt(_str, _len);
      } else {
        System.arraycopy(_digits, 0, _digitLens[_len-1], 0, _len);
        VALUE.set(_str, _digitLens[_len-1]);
      }
      return Double.parseDouble(_str);
    } catch( Exception e ) {
      return Double.NaN;
    }
  }
}
