package water.parser;

import java.util.Iterator;

import water.*;

import com.google.common.base.Objects;

public class SeparatedValueParser implements Iterable<double[]>, Iterator<double[]> {
  private final Key _key;
  private final long _startChunk;

  private Value _curVal;
  private long _curChunk;
  private byte[] _curData;
  private int _offset;

  private final char _separator;
  private final DecimalParser _decimal;
  private final double[] _fieldVals;

  public SeparatedValueParser(Key k, char seperator, int numColumnsGuess) {
    _decimal = new DecimalParser();
    _fieldVals = new double[numColumnsGuess];
    _separator = seperator;

    _key = k;
    _curVal = UKV.get(k);
    assert _curVal != null;

    if( _key._kb[0] == Key.ARRAYLET_CHUNK ) {
      _startChunk = _curChunk = ValueArray.getChunkIndex(_key);
    } else {
      _startChunk = _curChunk = -1;
    }

    _offset = 0;
    _curData = _curVal.get();
    if( _curChunk > 0 ) {
      while( hasNextByte() ) {
        byte b = getByte();
        if( isNewline(b) ) break;
        ++_offset;
      }
    }
    skipNewlines();

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
    if (_curVal == null) {
      _curChunk = -1;
      return false;
    }
    _offset = 0;
    _curData = _curVal.get(1024);
    return _offset < _curData.length;
  }

  private byte getByte() {
    assert hasNextByte();
    return _curData[_offset];
  }

  private void skipNewlines() {
    while( hasNextByte() ) {
      byte b = getByte();
      if( !isNewline(b) ) return;
      ++_offset;
    }
  }

  // According to the CSV spec, `"asdf""asdf"` is a legal quoted field
  // We are going to parse this, but not simplify it into `asdf"asdf`
  // we will make the simplifying parse of " starts and stops escaping
  private byte scanPastNextSeparator() {
    boolean escaped = false;
    while( hasNextByte() ) {
      byte b = getByte();
      if( b == '"' ) {
        escaped = !escaped;
      } else if( !escaped && isSeparator(b) ) {
        ++_offset;
        return b;
      }
      _decimal.addCharacter(b);
      ++_offset;
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
    return hasNextByte() && (
        _curChunk == _startChunk ||  // we have not finished the chunk
        (_curChunk == _startChunk + 1 && _offset == 0)); // we have not started the next
  }

  @Override
  public double[] next() {
    assert !isNewline(getByte());

    byte b;
    int field = 0;
    while( hasNextByte() ) {
      if( field < _fieldVals.length ) {
        _decimal.reset();
        b = scanPastNextSeparator();
        _fieldVals[field] = _decimal.doubleValue();
      } else {
        b = scanPastNextSeparator();
      }
      ++field;
      if( isNewline(b) ) {
        break;
      }
    }
    for(; field < _fieldVals.length; ++field) _fieldVals[field] = Double.NaN;
    skipNewlines();
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
