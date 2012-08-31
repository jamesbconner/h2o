package water.parser;

import java.util.Arrays;

import water.Key;

public class SVMLightParserKV extends CSVParserKV<double[]> {
  static ParserSetup setup() {
    ParserSetup s = new ParserSetup();
    s.separator = ':';
    s.whiteSpaceSeparator = true;
    s.collapseWhiteSpaceSeparators = true;
    s.defaultDouble = 0.0;
    s.skipFirstRecord = true;
    return s;
  }

  public SVMLightParserKV(Key k, int nchunks) {
    this(k, nchunks, 8);
  }

  public SVMLightParserKV(Key k, int nchunks, int ncolumns) {
    super(k, nchunks, new double[ncolumns], null, setup());
  }

  int _state = STATE_INITIAL;

  private static final int STATE_INITIAL = 1;
  private static final int STATE_COLNUM = 2;
  private static final int STATE_VALUE = 3;
  private static final int STATE_COMMENT = 4;
  private static final int STATE_IGNORE = 5;

  @Override
  protected void endField() throws IllegalArgumentException,
      IllegalAccessException {
    // is it comment?
    if (!_skipRecord) {
      switch (_state) {
      case STATE_INITIAL:
        Arrays.fill(_csvRecord, _setup.defaultFloat);
        _state = STATE_COLNUM;
        super.endField();
        break;
      case STATE_COLNUM:
        if (parseInt(_fieldStart, _fieldEnd, 10)) {
          if (_ival < _column)
            throw new Error("feature numbers must be strictly increasing!");
          if (_ival >= _csvRecord.length) {
            double[] newVal = new double[(_ival + 1) + (_ival >> 1)];
            Arrays.fill(newVal, _setup.defaultDouble);
            System.arraycopy(_csvRecord, 0, newVal, 0, _csvRecord.length);
            _csvRecord = newVal;
          }
          _state = STATE_VALUE;
          _skipRecord = true;
          super.endField();
          _skipRecord = false;
          _column = _ival;
        } else { // qid or #?
          byte b = (_fieldStart < _data.length) ? _data[_fieldStart]
              : _nextData[_fieldStart - _data.length];
          switch (b) {
          case '#':
            _state = STATE_COMMENT;
            break;
          case 'q': // 'qid'
            _state = STATE_IGNORE;
            break;
          default:
            System.err.println("unexpected column id at offset["
                + _currentOffset
                + ":"
                + _fieldStart
                + "], previous column = "
                + _column
                + " '"
                + new CSVString(_currentOffset + _fieldStart, _fieldEnd
                    - _fieldStart) + "'");
            _state = STATE_IGNORE;
          }
        }
        break;
      case STATE_VALUE:
        super.endField();
        _state = STATE_COLNUM;
        break;
      case STATE_COMMENT:
        break; // don't do anything here
      case STATE_IGNORE:
        _state = STATE_COLNUM;
        _skipRecord = true;
        super.endField();
        _skipRecord = false;
        break;
      default:
        throw new Error("Illegal state!");
      }
    } else
      super.endField();
  }

  @Override
  protected boolean endRecord() throws IllegalArgumentException,
      IllegalAccessException {
    _column = ncols();
    _state = STATE_INITIAL;
    return super.endRecord();
  }
}
