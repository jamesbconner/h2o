
package water.parser;

import java.util.ArrayList;
import java.util.Arrays;
import water.*;

/**
 *
 * @author peta
 */
public class FastParser extends MRTask {
  
  public static final class Row {
    public final double[] _fieldVals;

    public Row(int numOfColumns) {
      _fieldVals       = new double[numOfColumns];
    }

    public Row(double[] vals) {
      _fieldVals = vals;
    }

    @Override public String toString() {
      return Arrays.toString(_fieldVals);
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
  
  //public byte CHAR_DECIMAL_SEPARATOR = '.';
  //public byte CHAR_SEPARATOR = ',';

  public byte CHAR_DECIMAL_SEPARATOR = 0;
  public byte CHAR_SEPARATOR = 0;
  
  
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
  
  private static final long LARGEST_DIGIT_NUMBER = 1000000000000000000L;

  public final Key _aryKey;
  
  int _phase;
  
  int _numColumns;
  
  long[] _powersOf10 = new long[8];
  
  
  public FastParser(Key aryKey) {
    _aryKey = aryKey;
    _phase = 1;
    _powersOf10[0] = 10;
    _powersOf10[1] = 100;
    _powersOf10[2] = 1000;
    _powersOf10[3] = 10000;
    _powersOf10[4] = 100000;
    _powersOf10[5] = 1000000;
    _powersOf10[6] = 10000000;
    _powersOf10[7] = 100000000;
    _numColumns = 20; // TODO DELETE THIS WHEN I COMPUTE COLUMNS
    CHAR_SEPARATOR = ',';
    CHAR_DECIMAL_SEPARATOR = '.';
  }
  
  private void addError(long pos, String error) {
    
  }

  private void addWarning(long pos, String error) {
    
  }
  
  @Override public void map(Key key) {
    ValueArray _ary = null;
    FastTrie[] _columnTries = new FastTrie[10];
    for (int i = 0; i < _columnTries.length; ++i)
      _columnTries[i] = new FastTrie();
    byte[] bits = DKV.get(key).get();
    int offset = 0;
    int state = SKIP_LINE;
    byte quotes = 0;
    int colIdx = 0;
    FastTrie colTrie = null;
    long number = 0;
    long exp = 0;
    int number_multiplier = 0;
    int fractionDigits = 0;
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
          } if ((quotes != 0) || !(isEOL(c) || isWhitespace(c) || (c == CHAR_SEPARATOR))) {
            offset += colTrie.addByte(c); // FastTrie returns skipped chars - 1
            break NEXT_CHAR;
          }
          // fallthrough to STRING_END
        // ---------------------------------------------------------------------
        case STRING_END:
          // we have parsed the string enum correctly
          row._fieldVals[colIdx] = colTrie.getTokenId(); 
          state = SEPARATOR_OR_EOL; 
          // fallthrough to SEPARATOR_OR_EOL
        // ---------------------------------------------------------------------
        case SEPARATOR_OR_EOL:
          if (c == CHAR_SEPARATOR) {
            ++colIdx;
            if (colIdx==row._fieldVals.length) {
              addError(offset,"Only "+_numColumns+" columns expected.");
              break MAIN_LOOP;
            }
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          }
          if (isWhitespace(c))
            break NEXT_CHAR;
          // fallthrough to EOL
        // ---------------------------------------------------------------------
        case EOL: 
          if (colIdx != 0)
            if (_phase == 1)
              phase1AddRow(row);
            else 
              phase2AddRow(row);
          colIdx = 0;
          state = (c == CHAR_CR) ? EXPECT_COND_LF : WHITESPACE_BEFORE_TOKEN;
          if (secondChunk)
            break MAIN_LOOP; // second chunk only does the first row
          break NEXT_CHAR;
        // ---------------------------------------------------------------------
        case WHITESPACE_BEFORE_TOKEN:  
          if (isWhitespace(c))
            break NEXT_CHAR;
          if (c == CHAR_SEPARATOR) {
            // we have empty token, store as NaN
            row._fieldVals[colIdx++] = Double.NaN;
            if (colIdx==row._fieldVals.length) {
              addError(offset,"Only "+_numColumns+" columns expected.");
              break MAIN_LOOP;
            }
            break NEXT_CHAR;
          }
          // fallthrough to COND_QUOTED_TOKEN
        // ---------------------------------------------------------------------
        case COND_QUOTED_TOKEN:
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            quotes = c;
            state = TOKEN;
            break NEXT_CHAR;
          }
          // fallthrough to TOKEN
        // ---------------------------------------------------------------------
        case TOKEN:
          if ((c > '9') && (c != CHAR_DECIMAL_SEPARATOR)) {
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
          if (c == '-') {
            exp = -1;
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
            state = NUMBER_FRACTION;
            fractionDigits = offset;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          if (exp == -1) {
            number = -number;
            exp = 1;
          }
          // fallthrough to NUMBER_END
        // ---------------------------------------------------------------------
        case NUMBER_END:
          if ( c == quotes) {
            quotes = 0;
            break NEXT_CHAR;
          }
          if (isEOL(c) || isWhitespace(c) || (c ==  CHAR_SEPARATOR)) {
            System.out.println("  number "+number+", fraction digits "+fractionDigits+", exp "+exp);
            double r = number;
            if ((fractionDigits > 0) && (fractionDigits <=8))
              r = r * 1 / _powersOf10[fractionDigits-1];
            else if (fractionDigits > 8) 
              r = r * 1 / Math.pow(10, fractionDigits);
            if (exp != 1)
              r = r * Math.pow(10,exp);
            row._fieldVals[colIdx] = r;
            state = SEPARATOR_OR_EOL;
            continue MAIN_LOOP;
          } else {
            throw new Error("After number, only EOL, whitespace or a separator "+CHAR_SEPARATOR+" is allowed, but character "+(char)c+" found");
          }
        // ---------------------------------------------------------------------
        case NUMBER_SKIP:  
          if ((c >= '0') && (c <= '9')) {
            break NEXT_CHAR;
          } else if (c == CHAR_DECIMAL_SEPARATOR) {
            state = NUMBER_SKIP_NO_DOT;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_SKIP_NO_DOT:
          if ((c >= '0') && (c <= '9')) {
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_FRACTION:
          if ((c >= '0') && (c <= '9')) {
            number = (number*10)+(c-'0');
            if (number >= LARGEST_DIGIT_NUMBER) {
              if (fractionDigits!=0)
                fractionDigits = offset - fractionDigits;
              state = NUMBER_SKIP;
            }
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            break NEXT_CHAR;
          }
          state = NUMBER_END;
          if (fractionDigits!=0)
            fractionDigits = offset - fractionDigits-1;
          if (exp == -1) {
            number = -number;
            exp = 1;
          }
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_EXP_START:
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
          if (c == '-') {
            state = NUMBER_EXP_NEGATIVE;
            break NEXT_CHAR;
          } else {
            state = NUMBER_EXP;
            if (c == '+')
              break NEXT_CHAR;
          }
          // fallthrough to NUMBER_EXP
        // ---------------------------------------------------------------------
        case NUMBER_EXP:
          if ((c >= '0') && (c <= '9')) {
            exp = (exp*10)+(c-'0');
            break NEXT_CHAR;
          }
          state = NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_EXP_NEGATIVE:
          if ((c >= '0') && (c <= '9')) {
            exp = (exp*10)+(c-'0');
            break NEXT_CHAR;
          }
          exp = - exp;
          state = NUMBER_END;
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

  @Override public void reduce(DRemoteTask drt) {
/*    FastParser other = (FastParser) drt;
    if (_columnTries == null) 
      _columnTries = other._columnTries;
    else
      for (int i = 0; i < _columnTries.length; ++i)
        _columnTries[i].update(other._columnTries[i]); */
  }
  
  private void phase1AddRow(Row row) {
    System.out.println(row.toString());
  }

  private void phase2AddRow(Row row) {
    System.out.println(row.toString());
    
  }
  
  private byte[] getFirstChunk() {
    Value v = DKV.get(_aryKey);
    if (v instanceof ValueArray) 
      return DKV.get(ValueArray.getChunk(_aryKey,0)).get();
    else 
      return v.get();
  }

  /** Determines the parser setup for separators. If the separators cannot be
   * inferred from the first chunk, the defaults - comma for separator and dot
   * for decimal separator are used. 
   */
  private void determineParserDelimiters() {
    byte[] bits = getFirstChunk();
    int offset = 0;
    while ((offset < bits.length) && ((CHAR_SEPARATOR == 0) || (CHAR_DECIMAL_SEPARATOR == 0))) {
      
      
      ++offset;
    }
    if (CHAR_SEPARATOR == 0) {
      addWarning(0,"Unable to determine separator character. Defaulting to comma");
      CHAR_SEPARATOR = ',';
    }
    if (CHAR_DECIMAL_SEPARATOR == 0) {
      addWarning(0,"Unable to determine decimal separator character. Defaulting to dot");
      CHAR_DECIMAL_SEPARATOR = '.'; 
    }
  }

  private static final int NUMBER_FOUND_OR_ERROR = -2;
  
  /** Determines the column names to be used for the parser, if any. Column
   * names are used if the first line has only strings in it and the second line
   * does not. Otherwise we assume that column names are not present.
   */
  private boolean determineColumnNames() {
    ArrayList<String> colNames = new ArrayList();
    byte[] bits = getFirstChunk();
    int offset = 0;
    while (offset < bits.length) {
      offset = parseColName(bits,offset,colNames);
      if (offset == NUMBER_FOUND_OR_ERROR)
        return false;
      if (isEOL(bits[offset]))
        break;
    }
    ++offset;
    if (offset >= bits.length)
      return false;
    if (bits[offset] == CHAR_LF)
      ++offset;
    
    
    
    
    return true;
      
  }
  
  private int tokenDelimiter(byte[] bits, int offset) {
    byte quotes = (bits[offset] == CHAR_SINGLE_QUOTE) || (bits[offset] == CHAR_SINGLE_QUOTE) ? bits[offset++] : 0;
    while (offset < bits.length) {
    }
    return offset;
  }
  
  
  private int parseColName(byte[] bits, int offset, ArrayList<String> colNames) {
    byte quotes = 0;
    int state = COND_QUOTED_TOKEN;
    byte c = bits[offset];
    StringBuilder sb = new StringBuilder();
MAIN_LOOP:    
    while (true) {
NEXT_CHAR:
      switch (state) {
        case COND_QUOTED_TOKEN:
          state = TOKEN;
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            quotes = c;
            break NEXT_CHAR;
          }
          if ((c >= '0') && (c <= '9'))
            return NUMBER_FOUND_OR_ERROR;
          state = STRING;
          // fallthrough to STRING
        case STRING:
          if (c == quotes)
            state = COND_QUOTE;
          if (isEOL(c) || (c == CHAR_SEPARATOR)) {
            state = STRING_END;
          } else {
            sb.append(c);
            break NEXT_CHAR;
          }
          // fallthrough to STRING_END
        case STRING_END:
          colNames.add(sb.toString());
          return offset;
        case COND_QUOTE:
          if (c == quotes) {
            sb.append(c);
            state = TOKEN;
            break NEXT_CHAR;
          }
          state = STRING_END;
          continue MAIN_LOOP;
        default:
          assert (false) : " We have wrong state "+state;
      }
      ++offset;
      if (offset == bits.length)
        return NUMBER_FOUND_OR_ERROR;
    }
  }
  
  

}
