
package water.parser;

import java.util.ArrayList;
import java.util.Arrays;

import water.*;
import water.parser.ParseDataset.DParseTask;

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


  DParseTask callback;


  public FastParser(Key aryKey, int numColumns, byte separator, byte decimalSeparator, DParseTask callback) throws Exception {
    _aryKey = aryKey;
    _numColumns = numColumns;
    CHAR_SEPARATOR = separator;
    CHAR_DECIMAL_SEPARATOR = decimalSeparator;
    this.callback = callback;
  }

  public final void parse(Key key, boolean skipFirstLine) throws Exception {
    ValueArray _ary = null;
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
    int tokenStart = 0; // used for numeric token to backtrace if not successful
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
            colTrie = callback._enums[colIdx];
            continue MAIN_LOOP;
          } else if (isEOL(c)) {
            state = EOL;
            continue MAIN_LOOP;
          }
          state = NUMBER;
          number = 0;
          fractionDigits = 0;
          numStart = offset;
          tokenStart = offset;
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
          if (isEOL(c) /* || isWhitespace(c) */ || (c ==  CHAR_SEPARATOR)) {
            exp = exp - fractionDigits;
            row.setCol(colIdx,number, (short) exp, (byte) numStart);
            state = SEPARATOR_OR_EOL;
            continue MAIN_LOOP;
          } else {
            offset = tokenStart-1;
            break NEXT_CHAR; // parse as String token now 
            //throw new Exception("After number, only EOL, whitespace or a separator "+CHAR_SEPARATOR+" is allowed, but character "+(char)c+" found");
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
      if (offset < 0) {
        assert secondChunk : "This can only happen when we are in second chunk and are reverting to first one.";
        secondChunk = false;
        key = _ary.make_chunkkey(ValueArray.getOffset(key));
        Value v = DKV.get(key); // we had the last key
        assert (v != null) : "The value used to be there!";
        bits = v.get();
        offset += bits.length;
      } else if (offset >= bits.length) {
        if (_ary == null)
          break;
        numStart -= bits.length;
        fractionDigits -= bits.length;
        offset -= bits.length;
        tokenStart -= bits.length;
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

  private static boolean isWhitespace(byte c) {
    return (c == CHAR_SPACE) || (c == CHAR_TAB);
  }

  private static boolean isEOL(byte c) {
    return (c == CHAR_CR) || (c == CHAR_LF) || (c == CHAR_VT) || (c == CHAR_FF);
  }

  private static final int TOKEN_START = 100;
  private static final int SECOND_LINE = 101;
  private static final int SECOND_COND_QUOTED_TOKEN = 102;
  private static final int SECOND_WHITESPACE_BEFORE_TOKEN = 103;
  private static final int SECOND_TOKEN_FIRST_LETTER = 104;
  private static final int SECOND_TOKEN = 105;
  private static final int SECOND_COND_QUOTE = 106;
  private static final int SECOND_SEPARATOR_OR_EOL = 107;


  static String [] determineColumnNames(byte[] bits, byte separator) {
    ArrayList<String> colNames = new ArrayList();
    int offset = 0;
    int state = COND_QUOTED_TOKEN;
    String [] result;
    byte quotes = 0;
    byte c = bits[offset];
    StringBuilder sb = null;
MAIN_LOOP:
    while (true) {
NEXT_CHAR:
      switch (state) {
        case WHITESPACE_BEFORE_TOKEN:
          if (c == CHAR_SPACE) {
            if (c == separator)
              break NEXT_CHAR;
          } else if (c == separator) {
            // we have empty token, store as empty string
            return null;
          }
          // fallthrough to COND_QUOTED_TOKEN
        case COND_QUOTED_TOKEN:
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            quotes = c;
            state = TOKEN_START;
            break NEXT_CHAR;
          }
          // fallthrough to TOKEN_START
        case TOKEN_START:
          if ((c >= '0') && (c <= '9'))
            return null; // we can't have the header - numeric in first row
          sb = new StringBuilder();
          state = TOKEN;
          // fallthrough to TOKEN
        case TOKEN:
          if ((quotes == 0) && ((c == separator) || isEOL(c))) {
            state = SEPARATOR_OR_EOL;
            continue MAIN_LOOP;
          } else if (c == quotes) {
            state = COND_QUOTE;
            break NEXT_CHAR;
          }
          sb.append((char)c);
          break NEXT_CHAR;
        case COND_QUOTE:
          if (c == quotes) {
            state = TOKEN;
            sb.append((char)c);
            break NEXT_CHAR;
          }
          quotes = 0;
          state = SEPARATOR_OR_EOL;
          // fallthrough to SEPARATOR_OR_EOL
        case SEPARATOR_OR_EOL:
          colNames.add(sb.toString());
          if (isEOL(c)) {
            state = (c == CHAR_CR) ? EXPECT_COND_LF : SECOND_LINE;
            break NEXT_CHAR;
          } else if (c == separator) {
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          } else {
            //throw new Exception("Separator or end of line expected after the end of column "+colNames.size()+" but "+c+" found");
            return null;
          }
        case EXPECT_COND_LF:
          state = SECOND_LINE;
          if (c == CHAR_LF)
            break NEXT_CHAR;
          // fallthrough to SECOND_LINE
        case SECOND_LINE:
          System.out.println("Found second line after "+colNames.size()+" columns");
         // _numColumns = colNames.size();
          state = SECOND_WHITESPACE_BEFORE_TOKEN;
          // fallthrough to SECOND_WHITESPACE_BEFORE_TOKEN
        case SECOND_WHITESPACE_BEFORE_TOKEN:
          if (c == separator)
              break NEXT_CHAR;
          // fallthrough SECOND_COND_QUOTED_TOKEN
        case SECOND_COND_QUOTED_TOKEN:
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            quotes = c;
            state = SECOND_TOKEN_FIRST_LETTER;
            break NEXT_CHAR;
          }
          // fallthrough SECOND_TOKEN_FIRST_LETTER
        case SECOND_TOKEN_FIRST_LETTER:
          if ((c >= '0') && (c <= '9')) // we have confirmed it was header
            break MAIN_LOOP;
          state = SECOND_TOKEN;
          // fallthrough SECOND_TOKEN
        case SECOND_TOKEN:
          if ((quotes == 0) && ((c == separator) || isEOL(c))) {
            state = SECOND_SEPARATOR_OR_EOL;
            continue MAIN_LOOP;
          } else if (c == quotes) {
            state = SECOND_COND_QUOTE;
            break NEXT_CHAR;
          }
          sb.append(c);
          break NEXT_CHAR;
        case SECOND_COND_QUOTE:
          if (c == quotes) {
            state = TOKEN;
            sb.append(c);
            break NEXT_CHAR;
          }
          quotes = 0;
          state = SECOND_SEPARATOR_OR_EOL;
          // fallthorugh to SECOND_SEPARATOR_OR_EOL
        case SECOND_SEPARATOR_OR_EOL:
          if (isEOL(c)) { // end of second line means all were strings again...
            return null;
          } else if (c == separator) {
            state = SECOND_TOKEN_FIRST_LETTER;
            break NEXT_CHAR;
          } else {
            // throw new Exception("Separator or end of line expected after the end of column "+colNames.size()+" but "+c+" found");
            return null;
          }
      }
      ++offset;
      if (offset == bits.length)
        break;
      c = bits[offset];
    }
    if(colNames.isEmpty())return null;
    result = new String[colNames.size()];
    colNames.toArray(result);
    return result;
  }


}




