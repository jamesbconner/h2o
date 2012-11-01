
package water.parser;

import java.util.ArrayList;
import java.util.Arrays;
import water.*;

/**
 *
 * @author peta
 */
public class FastParser {
  
  /**
   * Number is anything with number and exp being reasonable values. 
   * 
   * NaN is encoded as numLength -1
   * enum is encoded as numLength -2
   * 
   * 
   */
  public static final class Row {
    public final long[] _numbers;
    public final short[] _exponents;
    public final byte[] _numLength;

    public Row(int numOfColumns) {
      _numbers = new long[numOfColumns];
      _exponents = new short[numOfColumns];
      _numLength = new byte[numOfColumns];
    }
    
    public void setCol(int colIdx, long number, short exponent, byte numLength) {
      _numbers[colIdx] = number;
      _exponents[colIdx] = exponent;
      _numLength[colIdx] = numLength;
    }

    @Override public String toString() {
      return Arrays.toString(_numbers) + Arrays.toString(_exponents) + Arrays.toString(_numLength);
    }
  }
  
  
  public static final byte CHAR_TAB = '\t';
  public static final byte CHAR_LF = 10;
  public static final byte CHAR_SPACE = ' ';
  public static final byte CHAR_CR = 13;
  public static final byte CHAR_VT = 11;
  public static final byte CHAR_FF = 12;
  public static final byte CHAR_DOUBLE_QUOTE = '"';
  public static final byte CHAR_SINGLE_QUOTE = '\'';
  
  public final byte CHAR_DECIMAL_SEPARATOR;
  public final byte CHAR_SEPARATOR;
  
  private static final int SKIP_LINE = 0;
  private static final int EXPECT_COND_LF = 1;
  private static final int EOL = 2;
  private static final int TOKEN = 4;
  private static final int COND_QUOTED_TOKEN = 10;
  private static final int NUMBER = 5;
  private static final int NUMBER_SKIP = 50;
  private static final int NUMBER_SKIP_NO_DOT = 54;
  private static final int NUMBER_FRACTION = 51;
  private static final int NUMBER_EXP = 52;
  private static final int NUMBER_EXP_NEGATIVE = 61;
  private static final int NUMBER_EXP_START = 60;
  private static final int NUMBER_END = 53;
  private static final int STRING = 6;
  private static final int COND_QUOTE = 7; 
  private static final int SEPARATOR_OR_EOL = 8;
  private static final int WHITESPACE_BEFORE_TOKEN = 9;
  private static final int STRING_END = 11;
  private static final int COND_QUOTED_NUMBER_END = 12;
  
  private static final long LARGEST_DIGIT_NUMBER = 1000000000000000000L;

  public final Key _aryKey;
  
  public final int _numColumns;
  
  
  Object callback;
  
  
  public FastParser(Key aryKey, int numColumns, byte separator, byte decimalSeparator, Object callback) throws Exception {
    _aryKey = aryKey;
    _numColumns = numColumns; 
    CHAR_SEPARATOR = separator;
    CHAR_DECIMAL_SEPARATOR = decimalSeparator;
  }
  
  public final void parse(Key key, boolean skipFirstLine) throws Exception {
    ValueArray _ary = null;
    FastTrie[] _columnTries = new FastTrie[10];
    for (int i = 0; i < _columnTries.length; ++i)
      _columnTries[i] = new FastTrie();
    byte[] bits = DKV.get(key).get();
    int offset = 0;
    int state = skipFirstLine ? SKIP_LINE : WHITESPACE_BEFORE_TOKEN;
    byte quotes = 0;
    int colIdx = 0;
    FastTrie colTrie = null;
    long number = 0;
    int exp = 0;
    int fractionDigits = 0;
    int numStart = 0;
    boolean secondChunk = false;    
    Row row = new Row(_numColumns);
    byte c = bits[offset];
MAIN_LOOP:
    while (true) {
NEXT_CHAR:
      switch (state) {
        // ---------------------------------------------------------------------
        case SKIP_LINE: 
          if (isEOL(c)) {
            state = EOL;
          } else {
            break NEXT_CHAR;
          }
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case EXPECT_COND_LF:
          state = WHITESPACE_BEFORE_TOKEN;
          if (c == CHAR_LF)
            break NEXT_CHAR;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case STRING:
          if (c == quotes) {
            state = COND_QUOTE;
            break NEXT_CHAR;
          } 
          if ((quotes != 0) || ((!isEOL(c) && (c != CHAR_SEPARATOR)))) {
            offset += colTrie.addByte(c); // FastTrie returns skipped chars - 1
            break NEXT_CHAR;
          }
          // fallthrough to STRING_END
        // ---------------------------------------------------------------------
        case STRING_END:
          // we have parsed the string enum correctly
          row.setCol(colIdx, colTrie.getTokenId(),(short) 0, (byte) -2);
          state = SEPARATOR_OR_EOL; 
          // fallthrough to SEPARATOR_OR_EOL
        // ---------------------------------------------------------------------
        case SEPARATOR_OR_EOL:
          if (c == CHAR_SEPARATOR) {
            ++colIdx;
            if (colIdx == _numColumns)
              throw new Exception("Only "+_numColumns+" columns expected.");
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          }
          if (isWhitespace(c))
            break NEXT_CHAR;
          // fallthrough to EOL
        // ---------------------------------------------------------------------
        case EOL: 
          if (colIdx != 0)
//            System.out.println(row.toString());
            callback.addRow(row);
          colIdx = 0;
          state = (c == CHAR_CR) ? EXPECT_COND_LF : WHITESPACE_BEFORE_TOKEN;
          if (secondChunk)
            break MAIN_LOOP; // second chunk only does the first row
          break NEXT_CHAR;
        // ---------------------------------------------------------------------
        case WHITESPACE_BEFORE_TOKEN: 
          if (c == CHAR_SPACE) {
            if (c == CHAR_SEPARATOR)
              break NEXT_CHAR;
          } else if (c == CHAR_SEPARATOR) {
            // we have empty token, store as NaN
            row.setCol(colIdx++,-1,(short) 0, (byte) -2);
            if (colIdx == _numColumns)
              throw new Exception("Only "+_numColumns+" columns expected.");
            break NEXT_CHAR;
          }
          // fallthrough to COND_QUOTED_TOKEN
        // ---------------------------------------------------------------------
        case COND_QUOTED_TOKEN:
          state = TOKEN;
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            quotes = c;
            break NEXT_CHAR;
          }
          // fallthrough to TOKEN
        // ---------------------------------------------------------------------
        case TOKEN:
          if (((c > '9') || (c < '0')) && (c != CHAR_DECIMAL_SEPARATOR) && (c != '-') && (c != '+')) {
            state = STRING;
            colTrie = _columnTries[colIdx];
            continue MAIN_LOOP;
          } else if (isEOL(c)) {
            state = EOL;
            continue MAIN_LOOP;
          }  
          state = NUMBER;
          number = 0;
          fractionDigits = 0;
          numStart = offset;
          if (c == '-') {
            exp = -1;
            ++numStart;
            break NEXT_CHAR;
          } else {
            exp = 1;
          }
          // fallthrough to NUMBER
        // ---------------------------------------------------------------------
        case NUMBER:
          if ((c >= '0') && (c <= '9')) {
            number = (number*10)+(c-'0');
            if (number >= LARGEST_DIGIT_NUMBER)
              state = NUMBER_SKIP;
            break NEXT_CHAR;
          } else if (c == CHAR_DECIMAL_SEPARATOR) {
            ++numStart;
            state = NUMBER_FRACTION;
            fractionDigits = offset;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            ++numStart;
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
          // fallthrough to COND_QUOTED_NUMBER_END
        // ---------------------------------------------------------------------
        case COND_QUOTED_NUMBER_END:
          state = NUMBER_END;
          numStart = offset - numStart;
          if ( c == quotes) {
            quotes = 0;
            break NEXT_CHAR;
          }
          // fallthrough NUMBER_END
        case NUMBER_END:
          if (isEOL(c) || isWhitespace(c) || (c ==  CHAR_SEPARATOR)) {
            exp = exp - fractionDigits;
            row.setCol(colIdx,number, (short) exp, (byte) numStart);
            state = SEPARATOR_OR_EOL;
            continue MAIN_LOOP;
          } else {
            throw new Exception("After number, only EOL, whitespace or a separator "+CHAR_SEPARATOR+" is allowed, but character "+(char)c+" found");
          }
        // ---------------------------------------------------------------------
        case NUMBER_SKIP:  
          ++numStart;
          if ((c >= '0') && (c <= '9')) {
            break NEXT_CHAR;
          } else if (c == CHAR_DECIMAL_SEPARATOR) {
            state = NUMBER_SKIP_NO_DOT;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_SKIP_NO_DOT:
          ++numStart;
          if ((c >= '0') && (c <= '9')) {
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_FRACTION:
          if ((c >= '0') && (c <= '9')) {
            if (number >= LARGEST_DIGIT_NUMBER) {
              if (fractionDigits!=0)
                fractionDigits = offset - 1 - fractionDigits;
              state = NUMBER_SKIP;
            } else {
              number = (number*10)+(c-'0');
            }
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            ++numStart;
            if (fractionDigits!=0)
              fractionDigits = offset - 1 - fractionDigits;
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          if (fractionDigits!=0)
            fractionDigits = offset - fractionDigits-1;
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_EXP_START:
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
          if (c == '-') {
            ++numStart;
            state = NUMBER_EXP_NEGATIVE;
            break NEXT_CHAR;
          } else {
            state = NUMBER_EXP;
            if (c == '+') {
              ++numStart;
              break NEXT_CHAR;
            }
          }
          // fallthrough to NUMBER_EXP
        // ---------------------------------------------------------------------
        case NUMBER_EXP:
          if ((c >= '0') && (c <= '9')) {
            ++numStart;
            exp = (exp*10)+(c-'0');
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_EXP_NEGATIVE:
          if ((c >= '0') && (c <= '9')) {
            exp = (exp*10)+(c-'0');
            break NEXT_CHAR;
          }
          exp = - exp;
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case COND_QUOTE:
          if (c == quotes) {
            offset += colTrie.addByte(c); // FastTrie returns skipped chars - 1
            state = STRING;
            break NEXT_CHAR;
          } else {
            quotes = 0;
            state = STRING_END;
            continue MAIN_LOOP;
          }
        // ---------------------------------------------------------------------
        default:
          assert (false) : " We have wrong state "+state;
      } // end NEXT_CHAR
      ++offset;
      if (offset >= bits.length) {
        if (_ary == null)
          break;
        numStart -= bits.length;
        fractionDigits -= bits.length;
        offset -= bits.length;
        key = _ary.make_chunkkey(ValueArray.getOffset(key)+offset);
        Value v = DKV.get(key); // we had the last key
        if (v == null)
          break MAIN_LOOP;
        bits = v.get();
        secondChunk = true;
      }
      c = bits[offset];
    } // end MAIN_LOOP
  }
  
  private boolean isWhitespace(byte c) {
    return (c == CHAR_SPACE) || (c == CHAR_TAB);
  }
  
  private boolean isEOL(byte c) {
    return (c == CHAR_CR) || (c == CHAR_LF) || (c == CHAR_VT) || (c == CHAR_FF);
  }

}

/** Performs compression of the numbers. 
 * 
 * 
 * @author peta
 */
class ParserCompressor {
  
  
  final double[] _mins;
  final double[] _maxs;
  final int[] _scales;
  final byte[] _modes;
  final long[] _bases;
  
  
  private static final byte BYTE_ENUM = 1;
  private static final byte SHORT_ENUM = 2;
  private static final byte INT_ENUM = 3;
  private static final byte BYTE = 4;
  private static final byte BYTE_BASE = 5;
  private static final byte SHORT = 6;
  private static final byte SHORT_BASE = 7;
  private static final byte INT = 8;
  private static final byte INT_BASE = 9;
  private static final byte LONG = 10;
  private static final byte FLOAT = 11;
  private static final byte DOUBLE = 12;
  private static final byte SHORT_COMPRESSED_DOUBLE = 13;
  
  
  private static int[] powersOf10 = new int[] { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000 };

  
  byte[] _bits;
  int _offset;
  
  public ParserCompressor(double[] mins, double[] maxs, int[] scales, boolean[] isFloat) {
    _mins = mins;
    _maxs = maxs;
    _scales = scales;
    _modes = new byte[_mins.length];
    _bases = new long[_mins.length];
    calculateColumnEncodings(isFloat);
  }
  
  private void calculateColumnEncodings(boolean[] isFloat) {
    for (int i = 0; i < _mins.length; ++i) {
      if (_scales[i] == 0) { // it is integer value
        // does the column fit into byte 
        if (_maxs[i] - _mins[i] < 256) {
          if (_mins[i]>=0) {
            _modes[i] = BYTE;
          } else {
            _modes[i] = BYTE_BASE;
            _bases[i] = (int)_mins[i] - Byte.MIN_VALUE;
          }
        } else if (_maxs[i] - _mins[i] < 65536) {
          if (_mins[i]>=0) {
            _modes[i] = SHORT;
          } else {
            _modes[i] = SHORT_BASE;
            _bases[i] = (int)_mins[i] - Short.MIN_VALUE;
          }
        } else if (_maxs[i] - _mins[i] < (1l << 32)) {
          if (_mins[i]>=0) {
            _modes[i] = INT;
          } else {
            _modes[i] = INT_BASE;
            _bases[i] = (int)_mins[i] - Integer.MIN_VALUE;
          }
        } else {
          _modes[i] = LONG;
        }
      } else {
        double diff = (_maxs[i] * pow10(_scales[i]) - _mins[i] * pow10(_scales[i]));
        if (diff < 65536) {
          _modes[i] = SHORT_COMPRESSED_DOUBLE;
          _mins[i] = _mins[0] * pow10(_scales[i]); 
        } else {
          _modes[i] = isFloat[i] ? FLOAT : DOUBLE;
        }
      }
    }
  }
  
  private static int pow10(int exp) {
    int result = 1;
    int base = 10;
    while (exp>0) {
      if ((exp & 1) != 0)
        result *= base;
      exp >>= 1;
      base *= 10;
    }
    return result;
  }
  
  
  private void addRow(FastParser.Row row) {
    assert (_mins.length == row._numbers.length); 
    for (int i = 0; i < row._numbers.length; ++i) {
      long number = row._numbers[i];
      int exp = row._exponents[i];
      switch (_modes[i]) {
        case BYTE_ENUM:
          _offset = UDP.set1(_bits,_offset,(int) number);
          break;
        case SHORT_ENUM:
          _offset = UDP.set2(_bits,_offset,(int) number);
          break;
        case INT_ENUM:
          _offset = UDP.set4(_bits,_offset,(int) number);
          break;
        case BYTE:
          number = number * powersOf10[exp];
          _offset = UDP.set1(_bits,_offset,(int) number);
          break;
        case SHORT:
          number = number * powersOf10[exp];
          _offset = UDP.set2(_bits,_offset,(int) number);
          break;
        case INT:
          number = number * powersOf10[exp];
          _offset = UDP.set4(_bits,_offset,(int) number);
          break;
        case BYTE_BASE:
          number = number * powersOf10[exp] - _bases[i];
          _offset = UDP.set1(_bits,_offset,(int) number);
          break;
        case SHORT_BASE:
          number = number * powersOf10[exp] - _bases[i];
          _offset = UDP.set2(_bits,_offset,(int) number);
          break;
        case INT_BASE:
          number = number * powersOf10[exp] - _bases[i];
          _offset = UDP.set4(_bits,_offset,(int) number);
          break;
        case LONG:
          if (exp > powersOf10.length) 
            number = number * pow10(exp);
          else if (exp>0)
            number = number * powersOf10[exp];
          _offset = UDP.set8(_bits,_offset,(int) number);
          break;
        case FLOAT:
          _offset += UDP.set8d(_bits,_offset, (float) toDouble(number,exp));
          break;
        case DOUBLE:
          _offset += UDP.set8d(_bits,_offset, toDouble(number,exp));
          break;
        case SHORT_COMPRESSED_DOUBLE:
          double d = toDouble(number, exp+_scales[i]);
          _offset = UDP.set2(_bits,_offset,(int)(d - _mins[i]));
          break;
      }
    }
  }
  
  
  
}


