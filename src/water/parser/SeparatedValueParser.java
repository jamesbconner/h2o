package water.parser;

import hex.rf.Utils;

import java.util.Arrays;
import java.util.Iterator;

import water.Key;
import water.UKV;
import water.Value;
import water.ValueArray;
import water.parser.ParseDataset.ColumnDomain;

import com.google.common.base.Objects;

public class SeparatedValueParser implements Iterable<SeparatedValueParser.Row>, Iterator<SeparatedValueParser.Row> {
  private final Key _key;
  private final long _startChunk;

  private Value _curVal;
  private long _curChunk;
  private byte[] _curData;
  private int _offset;

  private final char _separator;
  private final DecimalParser _decimal;
  private final SloppyDecimalParser _sloppy_decimal;
  private final TextualParser _textual;
  private final Row _row;

  private final ColumnDomain[] _columnsDomains;

  public SeparatedValueParser(Key k, char seperator, int numColumnsGuess) {
    this(k,seperator,numColumnsGuess,null);
  }

  public SeparatedValueParser(Key k, char seperator, int numColumnsGuess, ColumnDomain[] columnsDomains ) {
    _sloppy_decimal = new SloppyDecimalParser();
    _decimal = new DecimalParser();
    _textual = new TextualParser();
    _row     = new Row(numColumnsGuess);

    _separator = seperator;
    _columnsDomains = columnsDomains;

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
      parse(b);
      ++_offset;
    }
    return '\n';
  }

  private void    parse(byte b) {_sloppy_decimal.addCharacter(b); _decimal.addCharacter(b); _textual.addCharacter(b); }
  private void    resetParsers() {_sloppy_decimal.reset(); _decimal.reset(); _textual.reset(); }
  private boolean isNewline(byte b)  { return b == '\r' || b == '\n'; }
  private boolean isSeparator(byte b) { return b == _separator || isNewline(b); }

  private void putToDictionary(int column, String key) {
    if( _columnsDomains != null && key != null && !"".equals(key) )
      _columnsDomains[column].add(key);
  }

  @Override
  public Iterator<SeparatedValueParser.Row> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return hasNextByte() && (
        _curChunk == _startChunk ||  // we have not finished the chunk
        (_curChunk == _startChunk + 1 && _offset == 0)); // we have not started the next
  }

  @Override
  public Row next() {
    assert !isNewline(getByte());

    byte b;
    int field = 0;
    while( hasNextByte() ) {
      if( field < _row._fieldVals.length ) {
        resetParsers();
        b = scanPastNextSeparator();
        double v = _sloppy_decimal.doubleValue();
        _row._fieldVals[field] = Double.isNaN(v) ? _decimal.doubleValue() : v;
        _row._fieldStringVals[field] = null;
        if (Double.isNaN(_row._fieldVals[field])) { // it is not a number => it can be a text field
          _row._fieldStringVals[field] = _textual.stringValue();
          putToDictionary(field, _row._fieldStringVals[field]);
        }
      } else {
        b = scanPastNextSeparator();
      }
      ++field;
      if( isNewline(b) ) {
        break;
      }
    }
    for(; field < _row._fieldVals.length; ++field) { _row._fieldVals[field] = Double.NaN; _row._fieldStringVals[field] = null; }
    skipNewlines();

    return _row;
  }

  public String toString() {
    return Objects.toStringHelper(this)
        .add("curChunk", _curChunk)
        .add("_offset", _offset) + "\n" +
        new String(_curData, _offset, Math.min(100, _curData.length - _offset));
  }


  @Override public void remove() { throw new UnsupportedOperationException(); }

  // Helper class to represent parsed row.
  // The driving attribute is _fieldVals which contains all parsed numbers.
  // In the case that it contains NaN then _fieldStringVals contains parsed field text
  public static final class Row {
    public final double[] _fieldVals;
    public final String[] _fieldStringVals;

    public Row(int numOfColumns) {
      _fieldVals       = new double[numOfColumns];
      _fieldStringVals = new String[numOfColumns];
    }

    @Override
    public String toString() {
      return Arrays.toString(_fieldVals) + "\n" + Arrays.toString(_fieldStringVals);
    }
  }
}
