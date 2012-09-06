package water.parser;

import java.util.Iterator;

import com.google.common.base.Objects;

import water.*;

public class SeparatedValueParser implements Iterable<double[]>, Iterator<double[]> {
  private final Key _key;
  private final Value _startVal;
  private final char _separator;
  
  private Value _curVal;
  private int _curChunk;
  private byte[] _curData;
  private int _offset;
  
  private final DecimalParser _decimal = new DecimalParser();
  private final double[] _fieldVals;
  private final int[] _fieldStarts;
  private final int[] _fieldEnds;

  public SeparatedValueParser(Key k, char seperator, int numColumnsGuess) {
    _separator = seperator;
    _startVal = UKV.get(k);
    assert _startVal != null;
    _key = _startVal._key;
    
    if( _key._kb[0] == Key.ARRAYLET_CHUNK ) {
      _curChunk = ValueArray.getChunkIndex(_key);
    } else {
      _curChunk = -1;
    }
    
    _curVal = _startVal;
    _offset = 0;
    _curData = _startVal.get();
    if( _curChunk > 0 ) scanToNextRow();
    
    _fieldVals = new double[numColumnsGuess];
    _fieldStarts = new int[numColumnsGuess];
    _fieldEnds = new int[numColumnsGuess];
  }
  
  // we are splitting this method, to clue in hotspot that the fast path
  // should be inlined
  private boolean hasNextByte() {
    if( _offset < _curData.length ) return true;
    return hasNextByteInSeparateChunk();
  }
  private boolean hasNextByteInSeparateChunk() {
    if( _curChunk < 0 ) return false;
    
    if( _offset < _curVal._max ) {
      _curData = _curVal.get(2*_curData.length);
      return true;
    }
    
    _curChunk += 1;
    Key k = ValueArray.getChunk(_key, _curChunk);
    _curVal = UKV.get(k);
    if (_curVal == null) return false;
    _offset = 0;
    _curData = _curVal.get(1024);
    return _offset < _curData.length;
  }
  
  private byte getNextByte() {
    assert hasNextByte();
    return _curData[_offset++];
  }
  
  private byte skipNewlines() {
    while( hasNextByte() ) {
      byte b = getNextByte();
      if( !isNewline(b) ) return b;
    }
    return '\n';
  }
  
  private void scanToNextRow() {
    while( hasNextByte() ) {
      byte b = getNextByte();
      if( isNewline(b) ) break;
    }
  }
  
  // According to the CSV spec, `"asdf""asdf"` is a legal quoted field
  // We are going to parse this, but not simplify it into `asdf"asdf`
  // we will make the simplifying parse of " starts and stops escaping
  private byte scanToNextSeparator() {
    boolean escaped = false;
    while( hasNextByte() ) {
      byte b = getNextByte();
      if( b == '"' ) {
        escaped = !escaped;
      } else if( !escaped && isSeparator(b) ) {
        return b;
      }
      _decimal.addCharacter(b);
    }
    return '\n';
  }

  private boolean isNewline(byte b)  { return b == '\r' || b == '\n'; }
  private boolean isSeparator(byte b) { return b == _separator || isNewline(b); }

  @Override
  public Iterator<double[]> iterator() {
    return this;
  }
  
  @Override
  public boolean hasNext() {
    return _curVal == _startVal && hasNextByte();
  }
  
  @Override
  public double[] next() {
    int field = 0;
    byte b = skipNewlines();
    if( !isNewline(b) ) {
      _decimal.reset();
      _decimal.addCharacter(b);
      do {
        if( field < _fieldVals.length ) {
          _fieldStarts[field] = _offset;
          b = scanToNextSeparator();
          _fieldEnds[field] = _offset-1;
          _fieldVals[field] = _decimal.doubleValue();
        } else {
          b = scanToNextSeparator();
          _fieldVals[field] = Double.NaN;
        }
        ++field;
        if( isNewline(b) ) {
          break;
        }
        _decimal.reset();
      } while( hasNextByte() );
    }
    while( field < _fieldVals.length ) {
      _fieldVals[field] = Double.NaN;
      _fieldStarts[field] = _offset;
      _fieldEnds[field] = _offset;
      ++field;
    }
    return _fieldVals;
  }

  public String toString() {
    return Objects.toStringHelper(this)
        .add("curChunk", _curChunk)
        .add("_offset", _offset) + "\n" +
        new String(_curData, _offset, Math.min(100, _curData.length - _offset));
  }
  

  @Override public void remove() { throw new UnsupportedOperationException(); }
}
