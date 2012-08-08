package water.csv;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;

/**
 * Simplified version of CSVParser.
 * Parses either stream of arraylets or just a byte array.
 * 
 * @author tomas
 *
 * @param <T>
 */
public class CSVParserKV<T> implements Iterable<T>, Iterator<T> {

  public static final int FILL_PARTIAL_RECORDS_WITH_DEFAULTS = 100;
  public static final int DROP_PARTIAL_RECORDS = 101;
  public static final int ERROR_ON_PARTIAL_RECORDS = 102;

  enum DataType {
    typeNull, typeInt, typeFloat, typeDouble, typeString
  };

  
  
  public final class CSVString implements CharSequence, Comparable<String>{
    long _offset;
    int _length;
        
    public CSVString() {}
    public CSVString(long offset, int length) {_offset = offset; _length = length;}
    
    public String toString(){
      int chunkIdx = (int)(_offset >> ValueArray.LOG_CHK);
      int chunkOffset = (int)(_offset & ((1 << ValueArray.LOG_CHK) -1));
      Key k = ValueArray.getChunk(_key,chunkIdx);
      Value v = DKV.get(k);
      byte [] res = v.get(chunkOffset + _length);
      if(res.length >= (chunkOffset + _length)){
        return new String(res,chunkOffset,_length);
      } else { // we need another chunk
        k = ValueArray.getChunk(_key,chunkIdx+1);
        v = DKV.get(k);
        int len = chunkOffset + _length - res.length;
        byte [] res2 = v.get(len);
        return new String(res,chunkOffset, res.length - chunkOffset) + new String(res2,0,len);
      }      
    }

    @Override
    public char charAt(int i) {
      int chunkIdx = i + (int)(_offset >> ValueArray.LOG_CHK);
      int chunkOffset = (int)(_offset & ((1 << ValueArray.LOG_CHK) -1));
      Key k = ValueArray.getChunk(_key,chunkIdx);
      Value v = DKV.get(k);
      byte [] res = v.get(chunkOffset);
      if(res.length > (chunkOffset)){
        return (char)res[chunkOffset];
      } else { // we need another chunk
        k = ValueArray.getChunk(_key,chunkIdx+1);
        v = DKV.get(k);
        byte [] res2 = v.get(chunkOffset-res.length+1);
        return (char)res2[chunkOffset - res.length];
      }      
    }
    int columns(){return _column;}
    @Override
    public int length() {
      return _length;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return new CSVString(_offset + start, end - start);
    }

    public int compareTo(byte [] bytes){      
      int chunkIdx = (int)(_offset >> ValueArray.LOG_CHK);
      int chunkOffset = (int)(_offset & ((1 << ValueArray.LOG_CHK) -1));
      Key k = ValueArray.getChunk(_key,chunkIdx);
      Value v = DKV.get(k);
      byte [] data = v.get(chunkOffset);
      int len = Math.min(_length, bytes.length);
      
      int res = 0;
      if(data.length > (chunkOffset + len)){        
        for(int i = 0; i < len; ++i){
          if((res = (data[i] - bytes[chunkOffset + i])) != 0) return res;
        }                
      } else { // we need another chunk
        for(int i = chunkOffset; i < data.length; ++i){
          if((res = (data[i] - bytes[i])) != 0) return res;
        }
        int off = data.length - chunkOffset;         
        k = ValueArray.getChunk(_key,chunkIdx+1);
        v = DKV.get(k);
        byte [] data2 = v.get(len-off);
        for(int i = 0; i < (len-off); ++i){
          if((res = (data2[i] - bytes[off+i])) != 0) return res;
        }
      }      
      return (_length - bytes.length);
    }
    
    @Override
    public int compareTo(String o) {
      return compareTo(o.getBytes());
    }    
    public boolean equals(String s){
      return (_length == s.length()) && (compareTo(s) == 0);
    }
  }

  public static class ParserSetup {
    public boolean skipFirstRecord = true; // applies ononly 
    public boolean parseColumnNames = false;
    public boolean whiteSpaceSeparator = false;
    public boolean collapseWhiteSpaceSeparators = true;
    public boolean ignoreEmptyRecords = true;
    public int partialRecordPolicy = FILL_PARTIAL_RECORDS_WITH_DEFAULTS;
    public char separator = ',';
    public int defaultInt = Integer.MAX_VALUE;
    public float defaultFloat = Float.NaN;
    public double defaultDouble = Double.NaN;
  }
  
  final ParserSetup _setup;

  // input values variables 
  final Key _key; // key to start parsing from
  final int _minChunkIdx; // idx of the first chunk
  final int _maxChunkIdx; // idx of the last chunk + 1 (the first record will still be parsed if chunk exists

  final T _csvRecord; 
  Field [] _fields;
  
  CSVString [] _strFields;
  
  final boolean _isArray;
  boolean _next;
  DataType [] _columnTypes;
  boolean _fresh = true;
  byte[] _data;
  byte[] _nextData;
  int _dataPtr;

  // parsing state
  boolean _skipRecord;
  int _fieldStart;
  int _recordStart;
  int _state;
  int _column;

  int _nextChunkIdx;
  long _currentOffset;
  int _length;
  
  

  CSVString [] _columnNames;
  
  FloatingDecimal _floatingDecimal = new FloatingDecimal(false, 0, new char[64], 0, false);
  
  int _ival;

  public static int getNColumns(Key k){
    Value v = DKV.get(k);
    byte [] data = v.get(1024*128);
    if(v instanceof ValueArray){
      k = v.chunk_get(0);
      Value v2 = DKV.get(k);
      data = v2.get(1024*128);
    }
    return getNColumns(data);
  }
  
  public static int getNColumns(byte [] data){
    int [] rec = new int[1];
    CSVParserKV<int[]> p = new CSVParserKV<int[]>(data,rec,null);
    int ncolumns = -1;
    for(int i = 0; i < 10; ++i){
      if(!p.hasNext())break;
      p.next();
      if(p._column > ncolumns) ncolumns = p._column;
    }
    return ncolumns;
  }
  
  
  @SuppressWarnings("unchecked")
  void initialize(String [] columns){
   
    if (_isArray) {
      _columnTypes = new DataType[Array.getLength(_csvRecord)];
      
      if (_csvRecord.getClass().getComponentType().equals(Integer.TYPE)) {
        Arrays.fill(_columnTypes, DataType.typeInt);
      } else if (_csvRecord.getClass().getComponentType().equals(Double.TYPE)) {
        Arrays.fill(_columnTypes, DataType.typeDouble);
      } else if (_csvRecord.getClass().getComponentType().equals(Float.TYPE)) {
        Arrays.fill(_columnTypes, DataType.typeFloat);
      } else if (CSVString.class
          .equals(_csvRecord.getClass().getComponentType())) {
        Arrays.fill(_columnTypes, DataType.typeString);
        _strFields = new CSVParserKV.CSVString[_columnTypes.length];
        for(int i = 0; i < _strFields.length; ++i){
          _strFields[i] = new CSVString();
          Array.set(_csvRecord,i,_strFields[i]);
        }
      } else {
        throw new UnsupportedOperationException();
      }
    } else if (_csvRecord.getClass().isInstance(Collection.class)) {
      throw new UnsupportedOperationException();
    } else {      
      _fields = new Field[columns.length];
      _columnTypes = new DataType[columns.length];
      int i = 0;
      ArrayList<CSVParserKV.CSVString> strFields = new ArrayList<CSVParserKV.CSVString>();
      try{
        for (String colName : columns) {
          if (colName != null) {
            _fields[i] = _csvRecord.getClass().getDeclaredField(colName);
            Type t = _fields[i].getGenericType();
            if (Integer.TYPE.equals(t)) {
              _columnTypes[i] = DataType.typeInt;
            } else if (Double.TYPE.equals(t)) {
              _columnTypes[i]  = DataType.typeDouble;
            } else if (Float.TYPE.equals(t)) {
              _columnTypes[i]  = DataType.typeFloat;
            } else if (CSVString.class.equals(t)) {
              _columnTypes[i]  = DataType.typeString;
              CSVString str = new CSVString();
              _fields[i].set(_csvRecord, str);
              strFields.add(str);            
            } else { // no parser available for this type
              throw new UnsupportedOperationException();
            }
          } else {
            _columnTypes[i] = DataType.typeNull;
          }
          ++i;
        }
      }catch(Exception e){throw new Error(e);}
      _strFields = new CSVParserKV.CSVString[strFields.size()];
      _strFields = strFields.toArray(_strFields);
    }        
    if(_setup.parseColumnNames){
      _columnNames = new CSVParserKV.CSVString[_columnTypes.length];
      _setup.parseColumnNames = false;
      _setup.skipFirstRecord = false;
      CSVParserKV<CSVString[]> p = new CSVParserKV<CSVString[]>(_data,_columnNames,null,_setup);      
      p.next();
      _setup.parseColumnNames = true;
      _dataPtr = p._dataPtr;
    } 
     _skipRecord = _setup.skipFirstRecord && (_nextChunkIdx > 1);
  }
  
  public CSVParserKV(Key k, int nchunks, T csvRecord, String [] columns) {
    this(k,nchunks,csvRecord, columns, new ParserSetup());
  }
  
  public CSVParserKV(Key k, int nchunks, T csvRecord, String [] columns, ParserSetup setup) {
    // first set the data
    Value v = DKV.get(k);    
    if(v != null) {
      if(v instanceof ValueArray){
        _key = ((ValueArray)v).chunk_get(0);        
        v = DKV.get(_key);
      } else
        _key = k;      
      _data = v.get();
      _length = _data.length;
      if(_key._kb[0] == Key.ARRAYLET_CHUNK){
        _currentOffset = ValueArray.getOffset(_key);
        _minChunkIdx = ValueArray.getChunkIndex(_key);
        _maxChunkIdx = _minChunkIdx + nchunks;
        _nextChunkIdx = _minChunkIdx+1;
        Key nextChunk = ValueArray.getChunk(_key, _nextChunkIdx);
        v = DKV.get(nextChunk);
        if(v != null){
          _nextData = (_nextChunkIdx < _maxChunkIdx)?v.get():v.get(1024); 
          _length += _nextData.length;
        }
      } else {
        _minChunkIdx = 0; 
        _maxChunkIdx = 0;
        _nextChunkIdx = 0;
      }      
    } else {
      _minChunkIdx = 0; 
      _maxChunkIdx = 0;
      _key = null;
    }
    _setup = setup;
    _setup.parseColumnNames = _setup.parseColumnNames && (_minChunkIdx == 0); 
    _csvRecord = csvRecord;
    _isArray = _csvRecord.getClass().isArray(); 
    initialize(columns);        
  }
  
  public CSVParserKV(byte [] data, T csvRecord, String [] columns) {
    this(data,csvRecord, columns, new ParserSetup());
  }
  
  public CSVParserKV(byte [] data, T csvRecord, String [] columns, ParserSetup setup) {
    // first set the data
    _data = data;
    _minChunkIdx = 0; 
    _maxChunkIdx = 1;
    _key = null;    
    _setup = setup;
    _setup.parseColumnNames = _setup.parseColumnNames && (_minChunkIdx == 0); 
    _csvRecord = csvRecord;
    _isArray = _csvRecord.getClass().isArray();
    if(_data != null)_length = _data.length;
    initialize(columns);        
  }
  
  private final int getDigit(char c) {
    int i = Integer.MAX_VALUE;
    if (Character.isLetterOrDigit(c)) {
      if (Character.isDigit(c)) {
        i = Character.getNumericValue(c);
      } else {
        i = Character.isUpperCase(c) ? (10 + c - 'A') : (10 + c - 'a');
      }
    } 
    return i;
  }
  
    
  protected boolean parseInt(int from, int to, int radix) {
    int sign = 1;
    int res = 0;
    int state = 0;
    
    for (int i = from; i < to; ++i) {
      byte b = (i < _data.length)?_data[i]:_nextData[i-_data.length];
      char ch = (char) b;
      switch (state) {
      case 0:
        if (Character.isWhitespace(ch))
          break;
        else if (ch == '-') {
          sign *= -1;
          break;
        }
        state = 1;
      case 1:
        if (Character.isWhitespace(ch))
          state = 2;
        else {
          int d = getDigit(ch);
          if(d >= radix)
            return false;
          res = radix * res + getDigit(ch);
        }
        break;        
      case 2:
        if (!Character.isWhitespace(ch))
          return false;        
      }
    }
    _ival = sign * res;
    return true;
  }
  

  protected boolean parseFloat(int from, int to) {
    int state = 0;
    
    _floatingDecimal.isNegative = false;
    _floatingDecimal.nDigits = 0;
    _floatingDecimal.decExponent = 0;
    int zeroCounter = 0;
    for (int i = from; i < to; ++i) {
      byte b = (i < _data.length)?_data[i]:_nextData[i-_data.length];
      char ch = (char) b;
      switch (state) {
      case 0:
        switch (ch) {
        case '.':
          state = 3;
          break;
        case '-':
          _floatingDecimal.isNegative = !_floatingDecimal.isNegative;
          break;
       
        case '0':
          _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
          ++_floatingDecimal.decExponent;
          state = 1;
          break;
        default:
          if(Character.isWhitespace(ch))
            continue; // trim the leading spaces
          if(!Character.isDigit(ch)){
            return false;
          }
          state = 2;
          _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
          ++_floatingDecimal.decExponent;
        }
        break;
      case 1: // leading zeros
        if (ch == '0')
          break; // ignore leading zeros
        // otherwise fall through
      case 2: // integer part
        switch (ch) {
        case '.':
          state = 3;
          break;
        case 'e':
        case 'E':
          state = 4;
          break;
        case ' ':
        case '\t':
          state = 5; // trim the trailing spaces
          break;
        default:
          if(!Character.isDigit(ch)){
            return false;
          }
          // too many digits
          if (_floatingDecimal.nDigits == _floatingDecimal.digits.length){
            return false;
          }              
          _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
          ++_floatingDecimal.decExponent;
        }
        break;
      case 3: // after decimal point
        switch (ch) {
     
        case '0':
          ++zeroCounter;
          break;
        case 'e':
        case 'E':
          state = 4;
          break;
        default:
          if(Character.isWhitespace(ch)){
            state = 5;
            break;
          }
          if(!Character.isDigit(ch)){
            return false;
          }
          while (zeroCounter > 0) {
            _floatingDecimal.digits[_floatingDecimal.nDigits++] = '0';
            --zeroCounter;
          }
          if (_floatingDecimal.nDigits == _floatingDecimal.digits.length){
            return false;
          }
          _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
        }
        break;
      case 4: // parse int and add it to the exponent
        if(parseInt(i, to, 10)) _floatingDecimal.decExponent += _ival;
        return (_floatingDecimal.nDigits > 0);
      case 5: // should be trailing spaces...ignore them but throw exception
              // if anything else
        switch (ch) {
        case ' ':
        case '\t':
          break;
        default:
          return false;
        }
        break;
      }
    }
    return (_floatingDecimal.nDigits > 0);      
  }


  private boolean endRecord() throws IllegalArgumentException, IllegalAccessException {
    boolean res = !_skipRecord;
    if (res && _column < _columnTypes.length) {
      switch (_setup.partialRecordPolicy) {
      case FILL_PARTIAL_RECORDS_WITH_DEFAULTS:
        for (int i = _column; i < _columnTypes.length; ++i)
          endField(_dataPtr,_dataPtr);       
        break;
      case DROP_PARTIAL_RECORDS:
        res = false;
        break;
      case ERROR_ON_PARTIAL_RECORDS:
        throw new Error("partial record with " + _column + " columns");
      default:
        throw new Error("illegal _partialRecordPolicy value");
      }
    }       
    _recordStart = _dataPtr + 1;
    _fieldStart = _dataPtr + 1;    
    _skipRecord = false;
    if(!res) //if we ignore this record, we have to reset the column, otherwise next record would come out as all defaults
      _column = 0;  // however, if res is true, we want to keep current column number so that we know how many columns did we parse
    return res;
  }

  private void endField(int from, int to) throws IllegalArgumentException, IllegalAccessException{    
    if(!_skipRecord && (_column < _columnTypes.length)){
      switch(_columnTypes[_column]){
      case typeInt:
        int ival = (parseInt(from,to,10))?_ival:_setup.defaultInt;
        if(_isArray) 
          Array.setInt(_csvRecord, _column, ival); 
        else 
          _fields[_column].setInt(_csvRecord, ival);
        break;
      case typeFloat:
        float fval = (parseFloat(from,to))?_floatingDecimal.floatValue():_setup.defaultFloat;
        if(_isArray) 
          Array.setFloat(_csvRecord, _column, fval); 
        else 
          _fields[_column].setFloat(_csvRecord, fval);
        break;
      case typeDouble:
        double dval = (parseFloat(from,to))?_floatingDecimal.doubleValue():_setup.defaultDouble;
        if(_isArray) 
          Array.setDouble(_csvRecord, _column, dval); 
        else 
          _fields[_column].setDouble(_csvRecord, dval);
        break;        
      case typeString:
      {
        int stringIdx = 0;
        for(int i = 0; i < _column; ++i) 
          if(_columnTypes[i] == DataType.typeString) ++stringIdx;
        _strFields[stringIdx]._offset = _currentOffset + _fieldStart;
        _strFields[stringIdx]._length = to - from; 
      }
        break;
      }                 
    }    
    ++_column;
    _fieldStart = _dataPtr+1;
    _state = STATE_INITIAL;    
  }

  private static final int STATE_INITIAL = 104;
  private static final int STATE_QUOTED = 105;
  private static final int STATE_ENDQUOTE = 106;
  private static final int STATE_AFTER_QUOTED = 107;
  private static final int STATE_NONQUOTEDFIELD = 108;

  private boolean nextData() {
    if (_nextData == null || _nextChunkIdx == _maxChunkIdx)
      return false;
    _dataPtr -= _data.length;
    _data = _nextData;
    //System.out.println("processing chunk " + _nextChunkIdx);
    ++_nextChunkIdx;
    Key k = ValueArray.getChunk(_key, _nextChunkIdx);
    Value v = DKV.get(k);
    if (v != null){
      _nextData = (_nextChunkIdx == _maxChunkIdx) ? v.get(1024) : v.get();
      _currentOffset = ValueArray.getOffset(k);
    } else _nextData = null;
    _length = _data.length + ((_nextData == null) ? 0 : _nextData.length);
    return true;
  }
  
  final public boolean isSeparator(char c){
    return (c == _setup.separator) || (_setup.whiteSpaceSeparator && Character.isWhitespace(c)); 
  } 

  private final int getFieldEnd(){
    if((_dataPtr > 0) && ('\n' == (char)((_dataPtr < _data.length)?_data[_dataPtr]:_nextData[_dataPtr-_data.length]))){
      char pC = (char)(((_dataPtr -1) < _data.length)?_data[_dataPtr-1]:_nextData[_dataPtr - _data.length - 1]);
      if(pC == '\r') return _dataPtr -1;
    }
    return _dataPtr;
  }
  @Override
  public boolean hasNext() {
    if (_next)
      return true;    
    if(_data == null) return false;
    if (_dataPtr >= _data.length && !nextData())
      return false;
    _state = STATE_INITIAL;
    _column = 0;
    _fieldStart = _dataPtr;
    _recordStart = _dataPtr;
    boolean recordFinished = false;
    try{
      // try to parse
      while (!recordFinished) {
        if (_dataPtr == _length && _nextData != null && _nextData.length == 1024) {
          // deal with the improbable case of record crossing boundary by more
          // than 1024 bits
          Key k = ValueArray.getChunk(_key, _nextChunkIdx);
          Value v = DKV.get(k);
          if (v != null) {
            _nextData = v.get();
            _length = _data.length + _nextData.length;
          }
        }
        if (_dataPtr == _length)
          return false; // reach the end of data
        char c = (char) ((_dataPtr < _data.length) ? _data[_dataPtr]
            : _nextData[_dataPtr - _data.length]);
        switch (_state) {
        case STATE_INITIAL: // start of a field or only whitespace read
          switch (c) {
          case '\n':
            recordFinished = endRecord();
            break;
          case '"':
            _state = STATE_QUOTED;
            break;
          default:
            if (isSeparator(c))              
                endField(_fieldStart,_dataPtr);            
            else if(!Character.isWhitespace(c))
              _state = STATE_NONQUOTEDFIELD;
          }
          break;
        case STATE_QUOTED:
          if (c == '"')
            _state = STATE_ENDQUOTE;
          break;
        case STATE_ENDQUOTE:
          if (c == '"') {
            _state = STATE_QUOTED;
            break;
          } else
            _state = STATE_AFTER_QUOTED; // fall through
        case STATE_AFTER_QUOTED:
          if (c == '\n') {                        
            endField(_fieldStart, getFieldEnd());
            recordFinished = endRecord();
          } else if ((c == _setup.separator) || (Character.isWhitespace(c) && _setup.whiteSpaceSeparator)) {
            endField(_fieldStart,_dataPtr);         
          } else 
            if((c != '\r') && !Character.isWhitespace(c)) throw new Error("illegal character after quoted field: '" + c + "'");
          break;
        case STATE_NONQUOTEDFIELD:
          if (c == '"')
            throw new Error(
                "field quoted after non-whitespace characters have been read");
          if (c == '\n') {
            endField(_fieldStart, getFieldEnd());
            recordFinished = endRecord();
          } else if (isSeparator(c)) {
            endField(_fieldStart,_dataPtr);
          } 
        }
        ++_dataPtr;
      }
    }catch (Exception e){
      throw new Error(e);
    }
    return (_next = true);
  }

  @Override
  public T next() {
    if (!hasNext())
      throw new NoSuchElementException();
    _fresh = false;
    _next = false;
    return _csvRecord;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    if (!_fresh && _data != null) {
      if ((_key != null) && (_key._kb[0] == Key.ARRAYLET_CHUNK)
          && (_nextChunkIdx > (_minChunkIdx + 1))) { // do we need to re-read
                                                     // beginning of the data?
        Value v = DKV.get(_key);
        if (v == null)
          throw new Error("value disapeared?");
        _data = v.get();
        _nextChunkIdx = _minChunkIdx + 1;
        Key k = ValueArray.getChunk(_key, _nextChunkIdx);
        v = DKV.get(k);
        if (v != null)
          _nextData = v.get(); // nextData can not be the last chunk in this
                               // case, already checked by the branch
      }
    }
    return this;
  }
}
