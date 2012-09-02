package water.parser;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Collection;

import org.omg.CORBA._PolicyStub;

/**
 * Simple CSVParser, parses InputStream assuming data in csv format.
 * 
 * Takes object and the array of attribute names (Java identifiers) of this
 * object which will be filled when parsing. Object data are rewritten upon each
 * call of the next() function, which returns true f it was successful. Right
 * now, it can parser records (or arrays) with any of the following data types:
 * int, float, double, String.
 * 
 * Data can be distributed to several streams, which can be pushed to the parser
 * via addData method. Records can cross the stream boundaries. However, this
 * WILL FAIL if quoted value including newline character crosses the stream
 * boundary in such a way that the newline character is in the second stream.
 * 
 * @author tomas
 */
public final class CSVParser {  
  Object _record; // golden object with csv record
  final CSVParserSetup _setup;
  boolean _closed; // set if no more data should be returned
  int _index = -1; // index of the current byte
  int _from = 0; // pointer of to the start of the field which is currently being processed
  int _to; // pointer of to the end of the field which is currently being processed

  // data is kept in a circular buffer so we can be using 2 buffers at the same time 
  // (needed for records crossing the obundary)  
  private byte[][] _data = new byte[2][];

  // current data buffer
  final byte[] data() {
    return _data[_dataIdx];
  }

  // next data buffer
  final byte[] nextData() {
    return _data[(_dataIdx + 1) & 1];
  }

  // get byte of the given index 
  final byte getByte(int index) {
    if ((index < 0) || (index >= _length))
      throw new ArrayIndexOutOfBoundsException(index);
    return (index < data().length) ? data()[index] : nextData()[index
        - data().length];
  }

  int _dataIdx; // index into circular buffer with data buffers 
  // (currently of size 2 so only 0 or 1 are valid values)

  int _length; // _data.length + _nextData.length 
  int _column; // current column index
  int _state = 0; 

  // newline flag is set after reading \r charcter so that \r\n is treated as a single newline
  // while \r by itself is added to the field's value (e.g. for binary data)
  boolean _newLineFlag = false; 
  // final Map<String, IObjectParser> _parsers;
  final static int NEWLINE_STATE = 1000;

  boolean _dontParseFlag; // set if skipping partial record
  boolean _resetParsers = false; // set if the parsers states should be reset before parsing next field 

  final String[] _columnNames; // array with parsed column names (or null if no column names were parsed)
  final String[] _columns; // array with attribute names of the csv record corresponding to columns in csv data

  final FieldValueParser[] _columnParsers; 
  Field[] _fields;

  /**
   * Creates the CSVParser
   * 
   * @param data
   *          - first array with input data
   * @param csvRecord
   *          - record object which will be filled upon each successful call of
   *          next method. Can be either an array or an arbitrary object with
   *          attribute names specified in columns arg.
   * @param columns
   *          - in case of filling a Java object, this argument contains
   *          attribute names (Java ids) which will be filled by the next()
   *          method. They MUST be in the same order as the columns in csv file.
   * @param setup
   *          - CSVParser setup, pass this if you want to override default
   *          settings
   * 
   * @author tomas
   */
  public CSVParser(byte[] data, Object csvRecord, String[] columns,
      CSVParserSetup setup) throws NoSuchFieldException, SecurityException,
      CSVParseException, IOException, IllegalArgumentException,
      IllegalAccessException {
    _setup = setup;
    _column = 0;
    _length = 0;
    _columns = columns;
    _record = csvRecord;
    addData(data);
    int i = 0;
    FloatingDecimal f = new FloatingDecimal(false, 0, new char[64], 0, false);
    if (csvRecord.getClass().isArray()) {
      _columnParsers = new FieldValueParser[Array.getLength(csvRecord)];
      _columnNames = setup._parseColumnNames ? new String[Array
          .getLength(csvRecord)] : null;
      if (csvRecord.getClass().getComponentType().equals(Integer.TYPE)) {
        setArrayParser(new FieldValueParser(DataType.typeInt));
      } else if (csvRecord.getClass().getComponentType().equals(Double.TYPE)) {
        setArrayParser(new FieldValueParser(DataType.typeDouble, f));
      } else if (csvRecord.getClass().getComponentType().equals(Float.TYPE)) {
        setArrayParser(new FieldValueParser(DataType.typeFloat, f));
      } else if (CSVString.class
          .equals(csvRecord.getClass().getComponentType())) {
        setArrayParser(new FieldValueParser(DataType.typeCSVString));
      } else if (String.class.equals(csvRecord.getClass().getComponentType())) {
        setArrayParser(new FieldValueParser(DataType.typeString));
      } else {
        throw new UnsupportedOperationException();
      }
    } else if (csvRecord.getClass().isInstance(Collection.class)) {
      throw new UnsupportedOperationException();
    } else {
      _columnNames = _setup._parseColumnNames ? new String[_columns.length]
          : null;
      _fields = new Field[columns.length];
      _columnParsers = new FieldValueParser[columns.length];
      for (String colName : columns) {
        if (colName != null) {
          _fields[i] = csvRecord.getClass().getDeclaredField(colName);
          Type t = _fields[i].getGenericType();
          if (Integer.TYPE.equals(t)) {
            _columnParsers[i] = new FieldValueParser(DataType.typeInt);
          } else if (Double.TYPE.equals(t)) {
            _columnParsers[i] = new FieldValueParser(DataType.typeDouble, f);
          } else if (Float.TYPE.equals(t)) {
            _columnParsers[i] = new FieldValueParser(DataType.typeFloat, f);
          } else if (CSVString.class.equals(t)) {
            _columnParsers[i] = new FieldValueParser(DataType.typeCSVString);
            _fields[i].set(_record, _columnParsers[i]._csvString);
          } else if (String.class.equals(t)) {
            _columnParsers[i] = new FieldValueParser(DataType.typeString);
          } else { // no parser available for this type
            throw new UnsupportedOperationException();
          }
        } else {
          _columnParsers[i] = null;
        }
        ++i;
      }
    }
    initialize();
  }

  // depending on the setup, we may need to parse column names or skip the first (partial) 
  // record
  private void initialize() throws CSVParseException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException,
      IOException {
    if (_setup._parseColumnNames) {
      // parse column names
      CSVParserSetup s = new CSVParserSetup(_setup);
      s._parseColumnNames = false;
      s._skipFirstRecord = false;
      CSVParser p = new CSVParser(data(), _columnNames, null, s);
      if (!p.next()) {
        throw new CSVParseException(
            "unexpected exception while parsing header (column names) of the csv file");
      }
      _index = p._index;
      _from = p._index + 1;
      p = null;
    } else {
      if (_setup._skipFirstRecord) {
        _dontParseFlag = true;
        next();
        _dontParseFlag = false;
        _column = 0;
      }
    }
  }

  // helper method to set parser for each field of an array
  protected final void setArrayParser(FieldValueParser p) {
    for (int i = 0; i < _columnParsers.length; ++i) {
      _columnParsers[i] = new FieldValueParser(p);
      if (p._type == DataType.typeCSVString) {
        Array.set(_record, i, _columnParsers[i]._csvString);
      }
    }

  }

  /**
   * Add additional data to the parser. Previous state is preserved. 
   * @param d
   */
  public void addData(byte[] d) {
    if (data() == null)
      _data[_dataIdx] = d;
    else if (nextData() == null)
      _data[(_dataIdx + 1) & 1] = d;
    else if ((_index > data().length) && (_from > data().length)) {
      _index -= data().length;
      _from -= data().length; // recompute indexes
      _to -= data().length;
      _data[_dataIdx] = d;
      _dataIdx = (_dataIdx + 1) & 1;
      for (int i = 0; i < _column; ++i) {
        if (_columnParsers[i]._type == DataType.typeCSVString) { // recompute
                                                                 // string
                                                                 // boundaries
          if (_columnParsers[i]._csvString._offset < data().length)
            throw new Error(
                "Pushing more data before previous was parsed. Only two active buffers are allowed.");
          _columnParsers[i]._csvString._offset -= data().length;
        }
      }
    } else
      throw new Error(
          "Pushing more data before previous was parsed. Only two active buffers are allowed.");

    _length = ((_data[0] != null) ? _data[0].length : 0)
        + ((_data[1] != null) ? _data[1].length : 0);
  }
  
  // increase the current byte pointer if possible and return if succeeded
  private boolean nextByte() throws IOException {
    // System.out.println("index = " + _index + ", length = " + _length +
    // " ,_data.length = " + _data.length);      
    if(_index != (_length-1)){
      ++_index;
      return true;
    }  
    return false;
  }

  // set the attribute value and reset given parser
  protected boolean endField() throws IllegalArgumentException,
      IllegalAccessException, CSVParseException {
    boolean res = true;
    if (!_dontParseFlag) { 
      // don't parse flag means we're just skipping partial record -> do not try to parse the data
      if (_column >= _columnParsers.length) {
        if (_setup._ignoreAdditionalColumns)
          return true;
        throw new CSVParseException("Too many columns " + _column);
      }
      // parse the data
      if (_columnParsers[_column] != null) {
        // _columnParsers[_column].pushData(_data, _from, _to+1);
        switch (_columnParsers[_column]._type) {
        case typeInt:
          if (_record.getClass().isArray())
            Array.setInt(_record, _column,
                _columnParsers[_column].getIntVal(_from, _to - _from + 1));
          else
            _fields[_column].setInt(_record,
                _columnParsers[_column].getIntVal(_from, _to - _from + 1));
          break;

        case typeFloat: {
          float v = _columnParsers[_column].getFloatVal(_from, _to - _from + 1);
          if (_record.getClass().isArray())
            Array.setFloat(_record, _column, v);
          else
            _fields[_column].setFloat(_record, v);
          break;
        }
        case typeDouble: {
          double v = _columnParsers[_column].getDoubleVal(_from, _to - _from
              + 1);         
          if (_record.getClass().isArray())
            Array.setDouble(_record, _column, v);
          else
            _fields[_column].setDouble(_record, v);
          break;
        }
        case typeCSVString: {
          _columnParsers[_column]._csvString._offset = _from;
          _columnParsers[_column]._csvString._len = _to - _from + 1;
          // no need to do anything else here, CSVString object is already
          // shared with the current csv record
          break;
        }
        case typeString: {
          _columnParsers[_column]._csvString._offset = _from;
          _columnParsers[_column]._csvString._len = _to - _from + 1;
          String v = _columnParsers[_column]._csvString.toString();
          if (_record.getClass().isArray())
            Array.set(_record, _column, v);
          else
            _fields[_column].set(_record, v);
        }
          break;
        default:
          assert false; //houdl not get here
        }
        res = _columnParsers[_column]._parseOk;
      }      
      ++_column;
    }
    _from = _index + 1;
    _state = 0;
    _newLineFlag = false;
    return res;
  }

  // called after newline, in case of incomplete record, set all the remaining
  // values to their default values
  protected boolean endRecord() throws IllegalArgumentException,
      IllegalAccessException, CSVParseException {
    boolean result = true;    
    
    // set all unset fields to default values
    if (!_dontParseFlag && _column != _columnParsers.length) {
      switch(_setup._partialRecordPolicy){
      case throwException:
        throw new CSVParseException("Partial record with " + _column + " columns");
      case dropRecord:
        result= false;
        break;
      case fillWithDefaults:
        for (int i = _column; i != _columnParsers.length; ++i)
          endField();     
        break;
      }
    }
    _column = 0;
    _resetParsers = true;
    _state = 0;
    _from = _index+1;
    return result;
  }

  protected boolean addChar(char c) throws CSVParseException,
      IllegalArgumentException, IllegalAccessException {
    switch (_state) {
   
    case 0: // the beginning of a record or field or only whitespaces have been read so far
      if (c == _setup._separator) {
        if (!_setup._collapseSpaceSeparators)
          endField();
        break;
      } else if (c == '\n') {
        if (! _dontParseFlag && (_column == 0)) { 
          // ignore empty line, unless don't parse flag is set in which case we're probably searching for the first newline character and should stop right away!
          return false;
        }
        // if we separate by spaces there can be some additional spaces before
        // the newline
        // which should be ignored
        if (!_setup._collapseSpaceSeparators
            || _column != _columnParsers.length)
          endField();        
        return endRecord();
      } else if (Character.isWhitespace(c)) {
        if (_setup._trimSpaces)
          ++_from; // skip spaces at the beginning of a value
        else {
          _to = _index;
        }
        break;
      } else if (c == '"') {
        _state = 2;
        _from = _index + 1;
      } else {
        _state = 1;
        if (_setup._trimSpaces || _setup._collapseSpaceSeparators)
          _from = _index;
        else if (_column < _columnParsers.length &&  _columnParsers[_column] != null) {
          switch (_columnParsers[_column]._type) {
          case typeInt:
          case typeFloat:
          case typeDouble:
            _from = _index; // in case of number values, skip any spaces in the beginning 
            break;
          }
        }
        _to = _index;
      }
      break;
    case 1: // non-space character has been read before
      if ((c == _setup._separator) || (c == '\n')) {
        endField();
        if (c == '\n') {          
          return endRecord();
        }
      } else if (!Character.isWhitespace(c) || !_setup._trimSpaces) {
        // skip spaces at the beginning of a value
        _to = _index;
      }
      break;
    case 2: // quoted state! ignore anything but " is treated as data
      if (c == '"')
        _state = 3;
      else
        _to = _index;
      break;
    case 3: // either end of a quoted region or quote inside quoted region (quotes can be escaped by another ", so "" is treated as ")
      if (c == '"') {
        _to = _index;
        _state = 2; // double quote, add " ot the data and return to quoted state
      } else { // end of the quoted region, should just be end of the field/record with possible spaces
        if ((c == _setup._separator) || (c == '\n')) {
          endField();
          if (c == '\n') {            
            return endRecord();
          }
        } else if (Character.isWhitespace(c)) {
          _state = 4;
        } else {
          CSVString parsedString = new CSVString(_from, _index
              - _from, this);
          // CSVString parsedString = new CSVString(_index - 64, 128, this);
          throw new CSVParseException("unexpected character '" + c
              + "', parsed string = '" + parsedString.toString() + "'");
        }
      }
      break;
    case 4: // spaces appended to the end of a field, nothing else allowed here
      if ((c == _setup._separator) || (c == '\n')) {
        endField();
        if (c == '\n') {          
          return endRecord();
        }
      } else if (!Character.isWhitespace(c)) {
        throw new CSVParseException("unexpected character '" + c + "'");
      }
      break;
    default:
      throw new CSVParseException("unexpected state during CSVLine parsing");
    }
    return false;
  }

  /**
   * Parses next CSVRecord and returns true if csv record has been successfully
   * parsed. Note data previously returned by this method is rewritten.
   * 
   * Returns true if next record has been successfully parsed (ended by newline
   * character). Keeps its state, so if it returns true because the last record
   * did not finish, you can put in more data via setNextData method and
   * continue parsing from the same state.
   * 
   * Parsed data are stored into csvrecord object passed in a constructor.
   * 
   * @return true if new CSVRecord has been successfully parsed
   * @throws IOException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws CSVParseException
   * @throws SecurityException
   * @throws NoSuchFieldException
   */
  public boolean next() throws IOException, IllegalArgumentException,
      IllegalAccessException, CSVParseException, NoSuchFieldException,
      SecurityException {
    if(_closed)return false;
    if (_resetParsers) {
      for (FieldValueParser p : _columnParsers) {
        if (p != null)
          p.reset();
      }
      _resetParsers = false;
    }
    
    while (nextByte()) {
      char c;
      c = (char) getByte(_index);
      if ((_index == data().length) && (_state == 2))
        throw new CSVEscapedBoundaryException();

      // lines can be ended by \r\n or just \n, however, \r can also be part of
      // the field's value -> when encouter \r, do not pass it in and wait for
      // the next char, if it is \n, treat both as a single newline char,
      // otherwise pass \r to the underlying parser as such
      if (_newLineFlag) {
        _newLineFlag = false;
        if (c == '\n')
          _to = Math.min(_to, _index - 2);
        else
          addChar('\r');
      }
      if ((c == '\r') && (_state != 2)) {
        _newLineFlag = true;
        continue;
      }
      if (addChar(c)) {
        return true;
      }
    }
 
    // _columnParsers[_column].pushData(_data, _from, _to+1);
    return false;
  }

  public static class CSVParseException extends Exception {
    private static final long serialVersionUID = 1L;

    public CSVParseException(String msg) {
      super(msg);
    }
  }

  public static class CSVEscapedBoundaryException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public CSVEscapedBoundaryException() {
      super(
          "Encoutered chunk boundary inside escaped sequence.  Currently unimplemented");
    }
  }

  public final String[] columnNames() {
    return _columnNames;
  }

  /**
   * Reset the state of the parser to initial state and it's data to the passed data.
   * 
   * @param data
   * @throws NoSuchFieldException
   * @throws SecurityException
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws CSVParseException
   * @throws IOException
   */
  public void reset(byte[] data) throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException,
      CSVParseException, IOException {
    _index = -1;
    _from = 0;
    _dataIdx = 0;
    _closed = false;
    _data[0] = data;
    
    // _currentInput = 0;
    for (FieldValueParser p : _columnParsers) {
      if (p != null)
        p.reset();
    }
    _resetParsers = false;
    initialize();
  }

  /**
   * Close the parser so that no other data are parsed.
   * (unless reset() is called)
   */
  public void close() {
    _closed = true;
  }

  // types parser can process
  public enum DataType {
    typeInt, typeFloat, typeDouble, typeCSVString, typeString, typeCharSeq, typeObj;
  }

  // class responsible for parsing individual fields
  final class FieldValueParser {
    final DataType _type;
    CSVString _csvString;
    final FloatingDecimal _floatingDecimal;
    int _radix = 10;

    FieldValueParser(DataType t) {
      this(
          t,
          ((t == DataType.typeFloat) || (t == DataType.typeDouble)) ? new FloatingDecimal(
              0.0) : null);
    }

    FieldValueParser(DataType t, FloatingDecimal f) {
      _type = t;
      _floatingDecimal = f;
      _csvString = new CSVString(0, 0, CSVParser.this);
    }

    protected CSVParser csvParser() {
      return CSVParser.this;
    }

    /**
     * copy constructor. FloatingDecimal is SHARED.
     * 
     * @param other
     */
    FieldValueParser(FieldValueParser other) {
      if (CSVParser.this != other.csvParser())
        throw new UnsupportedOperationException(
            "copy constructor of FieldValueParser can only be applied on classes with the same CSVParser class");
      _type = other._type;
      _floatingDecimal = other._floatingDecimal;
      _csvString = new CSVString(other._csvString._offset,
          other._csvString._len, CSVParser.this);
    }
    
    private boolean _parseOk = true;
    
    //return true if the last value was parsed correctly (otherwise default value was returned)
    public boolean parseOk(){
      return _parseOk;      
    }
    // trims leading and trailing spaces and than parses the string into
    // FloatingDecimal. result is stored in _floatingDecimal var (so that 1
    // FloatingDecimal instance can be shared across the columns
    //
    // parsing is done so that all assumed digits (non-space nor '.' nor '-'
    // characters) are copied into the digits array of the _floatingDecimal
    //
    // exponent is computed based on the part of the string before '.' + the
    // explicit exponent expression (parsed as an integer) if present
    private final void parseFloat(int from, int len) {   
      _parseOk = true;
      int state = 0;
      final int N = from + len;
      _floatingDecimal.isNegative = false;
      _floatingDecimal.nDigits = 0;
      _floatingDecimal.decExponent = 0;
      int zeroCounter = 0;
      for (int i = from; i < N; ++i) {
        byte b = getByte(i);
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
          case ' ':
          case '\t':
            break; // trim the leading spaces
          case '0':
            _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
            ++_floatingDecimal.decExponent;
            state = 1;
            break;
          default:
            if(!Character.isDigit(ch)){
              _parseOk = false;
              return;
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
              _parseOk = false;
              return;
            }
            // too many digits
            if (_floatingDecimal.nDigits == _floatingDecimal.digits.length){
              _parseOk = false; // too many digits 
              return;
            }              
            _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
            ++_floatingDecimal.decExponent;
          }
          break;
        case 3: // after decimal point
          switch (ch) {
          case ' ':
          case '\t':
            state = 5;
            break;
          case '0':
            ++zeroCounter;
            break;
          case 'e':
          case 'E':
            state = 4;
            break;
          default:
            if(!Character.isDigit(ch)){
              _parseOk = false;
              return;
            }
            while (zeroCounter > 0) {
              _floatingDecimal.digits[_floatingDecimal.nDigits++] = '0';
              --zeroCounter;
            }
            if (_floatingDecimal.nDigits == _floatingDecimal.digits.length){
              _parseOk = false;
              return;
            }
            _floatingDecimal.digits[_floatingDecimal.nDigits++] = ch;
          }
          break;
        case 4: // parse int and add it to the exponent
          _floatingDecimal.decExponent += parseInt(i, N - i, 10);
          return;
        case 5: // should be trailing spaces...ignore them but throw exception
                // if anything else
          switch (ch) {
          case ' ':
          case '\t':
            break;
          default:
            _parseOk = false;
            return;
          }
          break;
        }
      }
      if (_floatingDecimal.nDigits == 0)
        _parseOk = false;
    }

    final double getDoubleVal(int from, int len) {
      parseFloat(from, len);        
      return _parseOk?_floatingDecimal.doubleValue():_setup._defaultDouble;
    }

    final float getFloatVal(int from, int len) {
      parseFloat(from, len);
      return _parseOk?_floatingDecimal.floatValue():_setup._defaultFloat;
    }

    final int parseInt(int offset, int len) {
      return parseInt(offset, len, _radix);
    }
    final int parseInt(int offset, int len, int radix) {
      _parseOk = true;
      int sign = 1;
      int res = 0;
      int state = 0;
      int N = offset + len;

      for (int i = offset; i < N; ++i) {
        byte b = getByte(i);
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
            if(d >= radix){
              _parseOk = false;
              return _setup._defaultInt;
            }              
            res = radix * res + getDigit(ch);
          }
          break;
          
        case 2:
          if (!Character.isWhitespace(ch)){
            _parseOk = false;
            return _setup._defaultInt;
          }
        }
      }
      return sign * res;
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

    // parse integer from substring
    final int getIntVal(int offset, int len) {
      int res = _setup._defaultInt;
      try {
        res = parseInt(offset, len);
      } catch (NumberFormatException e) {
        res = _setup._defaultInt;
      }
      return res;
    }

    void toString(StringBuilder bldr) {
      bldr.setLength(0);
      int N = _csvString._len + _csvString._offset;
      int M = Math.min(_data.length, N);
      for (int i = _csvString._offset; i < M; ++i)
        bldr.append((char) getByte(i));
    }

    @Override
    public String toString() {
      StringBuilder bldr = new StringBuilder();
      toString(bldr);
      return bldr.toString();
    }

    public void reset() {
      _csvString._offset = -1;
      _csvString._len = -1;
      _parseOk = true;
    }
  }

  /**
   * Represents parameters of CSVParser.
   * 
   * All values initialized to their defaults.
   * 
   * @author tomas
   * 
   */
  public static class CSVParserSetup {
    public enum PartialRecordPolicy {throwException, dropRecord, fillWithDefaults};
    
    public PartialRecordPolicy _partialRecordPolicy = PartialRecordPolicy.dropRecord;
    
    public double _defaultDouble = Double.NaN;
    public float _defaultFloat = Float.NaN;
    public int _defaultInt = Integer.MAX_VALUE;
            
    // column separator, can be any character that fits into one byte
    public byte _separator;
    // set this to true if the first line of the data contains column names,
    // applies only to chunk 0
    public boolean _parseColumnNames;
    // if true all fields will be trimmed (applies only to string since number
    // parsers by default trim their input)
    public final boolean _trimSpaces;
    // if set and the separator is a whitespace character, multiple spaces will
    // be treated as a single separator
    public boolean _collapseSpaceSeparators;
    // if false, records with fewer than expected number of columns will
    // trigger a CSVParse exception
    public final boolean _toleratePartialRecords;
    // if true, the first (probably partial) record will be skipped before
    // parsing
    public boolean _skipFirstRecord;
    // if false, lines with more columns than expected will trigger a CSVParse
    // exception
    public boolean _ignoreAdditionalColumns = false;

    public CSVParserSetup() {
      _separator = (byte) ',';
      _parseColumnNames = true;
      _trimSpaces = false;
      _collapseSpaceSeparators = false;
      _toleratePartialRecords = false;
      _skipFirstRecord = false;
    }

    public CSVParserSetup(byte sep, boolean collapse) {
      _separator = sep;
      _parseColumnNames = true;
      _trimSpaces = false;
      _collapseSpaceSeparators = collapse;
      _toleratePartialRecords = false;
      _skipFirstRecord = false;
    }

    public CSVParserSetup(CSVParserSetup other) {
      _separator = other._separator;
      _parseColumnNames = other._parseColumnNames;
      _trimSpaces = other._trimSpaces;
      _collapseSpaceSeparators = other._collapseSpaceSeparators;
      _toleratePartialRecords = other._toleratePartialRecords;
      _skipFirstRecord = other._skipFirstRecord;
      _partialRecordPolicy = other._partialRecordPolicy;
      _ignoreAdditionalColumns = other._ignoreAdditionalColumns;
    }
  }
}
