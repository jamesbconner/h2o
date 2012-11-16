
package water.parser;

import java.util.ArrayList;

import water.*;
import water.parser.ParseDataset.DParseTask;
import water.parser.ValueString;

/**
 *
 * @author peta
 */
public class CsvParser extends CustomParser {


  public final byte CHAR_DECIMAL_SEPARATOR;
  public final byte CHAR_SEPARATOR;

  private static final byte SKIP_LINE = 0;
  private static final byte EXPECT_COND_LF = 1;
  private static final byte EOL = 2;
  private static final byte TOKEN = 3;
  private static final byte COND_QUOTED_TOKEN = 4;
  private static final byte NUMBER = 5;
  private static final byte NUMBER_SKIP = 6;
  private static final byte NUMBER_SKIP_NO_DOT = 7;
  private static final byte NUMBER_FRACTION = 8;
  private static final byte NUMBER_EXP = 9;
  private static final byte NUMBER_EXP_NEGATIVE = 10;
  private static final byte NUMBER_EXP_START = 11;
  private static final byte NUMBER_END = 12;
  private static final byte STRING = 13;
  private static final byte COND_QUOTE = 14;
  private static final byte SEPARATOR_OR_EOL = 15;
  private static final byte WHITESPACE_BEFORE_TOKEN = 16;
  private static final byte STRING_END = 17;
  private static final byte COND_QUOTED_NUMBER_END = 18;
  private static final byte POSSIBLE_EMPTY_LINE = 19;

  private static final long LARGEST_DIGIT_NUMBER = 1000000000000000000L;

  public final Key _aryKey;

  public final int _numColumns;
  
  public final boolean _skipFirstLine;


  DParseTask callback;



  public CsvParser(Key aryKey, int numColumns, byte separator, byte decimalSeparator, DParseTask callback, boolean skipFirstLine) throws Exception {
    _aryKey = aryKey;
    _numColumns = numColumns;
    CHAR_SEPARATOR = separator;
    CHAR_DECIMAL_SEPARATOR = decimalSeparator;
    this.callback = callback;
    _skipFirstLine = skipFirstLine;
  }

  @SuppressWarnings("fallthrough")
  @Override public final void parse(Key key) throws Exception {
    ValueArray _ary = _aryKey == null ? null : (ValueArray) DKV.get(_aryKey);
    ValueString _str = new ValueString();
    byte[] bits = DKV.get(key).get();
    int offset = 0;
    int state = _skipFirstLine ? SKIP_LINE : WHITESPACE_BEFORE_TOKEN;
    int quotes = 0;
    long number = 0;
    int exp = 0;
    int fractionDigits = 0;
    int numStart = 0;
    int tokenStart = 0; // used for numeric token to backtrace if not successful
    boolean secondChunk = false;
    int colIdx = 0;
    byte c = bits[offset];
    callback.newLine();
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
          state = POSSIBLE_EMPTY_LINE;
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
            _str.addChar();
            break NEXT_CHAR;
          }
          // fallthrough to STRING_END
        // ---------------------------------------------------------------------
        case STRING_END:
          if ((c != CHAR_SEPARATOR) && ((c == CHAR_SPACE) || (c == CHAR_TAB)))
            break NEXT_CHAR;
          // we have parsed the string enum correctly
          if((_str._off + _str._length) >  _str._buf.length){ // crossing chunk boundary
            assert _str._buf != bits;
            _str.addBuff(bits);
          }
          callback.addStrCol(colIdx, _str);
          _str.set(null, 0, 0);
          ++colIdx;
          state = SEPARATOR_OR_EOL;
          // fallthrough to SEPARATOR_OR_EOL
        // ---------------------------------------------------------------------
        case SEPARATOR_OR_EOL:
          if (c == CHAR_SEPARATOR) {
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          }
          if (isWhitespace(c))
            break NEXT_CHAR;
          // fallthrough to EOL
        // ---------------------------------------------------------------------
        case EOL:
          if (colIdx != 0) {
            colIdx = 0;
            callback.newLine();
          }
          state = (c == CHAR_CR) ? EXPECT_COND_LF : POSSIBLE_EMPTY_LINE;
          if (secondChunk)
            break MAIN_LOOP; // second chunk only does the first row
          break NEXT_CHAR;
        // ---------------------------------------------------------------------
        case POSSIBLE_EMPTY_LINE:
          if (isEOL(c)) {
            if (c == CHAR_CR)
              state = EXPECT_COND_LF;
            break NEXT_CHAR;
          }
          state = WHITESPACE_BEFORE_TOKEN;
          // fallthrough to WHITESPACE_BEFORE_TOKEN
        // ---------------------------------------------------------------------
        case WHITESPACE_BEFORE_TOKEN:
          if ((c == CHAR_SPACE) || ( c == CHAR_TAB)) {
              break NEXT_CHAR;
          } else if (c == CHAR_SEPARATOR) {
            // we have empty token, store as NaN
            callback.addInvalidCol(colIdx);
            ++colIdx;
            break NEXT_CHAR;
          } else if (isEOL(c)) {
            callback.addInvalidCol(colIdx);
            state = EOL;
            continue MAIN_LOOP;
          }
          // fallthrough to COND_QUOTED_TOKEN
        // ---------------------------------------------------------------------
        case COND_QUOTED_TOKEN:
          state = TOKEN;
          if ((c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE)) {
            assert (quotes == 0);
            quotes = c;
            break NEXT_CHAR;
          }
          // fallthrough to TOKEN
        // ---------------------------------------------------------------------
        case TOKEN:
          if (((c >= '0') && (c <= '9')) || (c == '-') || (c == CHAR_DECIMAL_SEPARATOR) || (c == '+')) {
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
            // fallthrough
          } else {
            state = STRING;
            _str.set(bits, offset, 0);
            continue MAIN_LOOP;
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
          if ( c == quotes) {
            state = NUMBER_END;
            quotes = 0;
            break NEXT_CHAR;
          }
          // fallthrough NUMBER_END
        case NUMBER_END:
          if (c == CHAR_SEPARATOR) {
            exp = exp - fractionDigits;
            callback.addNumCol(colIdx,number,exp);
            ++colIdx;
            // do separator state here too
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          } else if (isEOL(c)) {
            exp = exp - fractionDigits;
            callback.addNumCol(colIdx,number,exp);
            // do EOL here for speedup reasons
            if (colIdx != 0) {
              colIdx = 0;
              callback.newLine();
            }
            state = (c == CHAR_CR) ? EXPECT_COND_LF : POSSIBLE_EMPTY_LINE;
            if (secondChunk)
              break MAIN_LOOP; // second chunk only does the first row
            break NEXT_CHAR;
          } else if ((c != CHAR_SEPARATOR) && ((c == CHAR_SPACE) || (c == CHAR_TAB))) {
            state = NUMBER_END;
            break NEXT_CHAR;
          } else {
            state = STRING;
            offset = tokenStart-1;
            _str.set(bits,offset,0);
            break NEXT_CHAR; // parse as String token now
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
            //TODO
            _str.set(bits, offset, 0);
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
      if (offset <= 0) {
        assert secondChunk : "This can only happen when we are in second chunk and are reverting to first one.";
        secondChunk = false;
        Value v = DKV.get(key); // we had the last key
        assert (v != null) : "The value used to be there!";
        bits = v.get();
        offset += bits.length;
        --offset;
        _str.set(bits,offset,0);
      } else if (offset >= bits.length) {
        secondChunk = true;
        // if we can't get further we might have been the last one and we must
        // commit the latest guy if we had one. 
        if (_ary == null) {
          if ((state != EXPECT_COND_LF) && (state != POSSIBLE_EMPTY_LINE)) {
            c = CHAR_LF;
            continue MAIN_LOOP;
          }
          break MAIN_LOOP;
        }
        numStart -= bits.length;
        if (state == NUMBER_FRACTION)
          fractionDigits -= bits.length;
        offset -= bits.length;
        tokenStart -= bits.length;
        Key k2 = _ary.make_chunkkey(ValueArray.getOffset(key)+ValueArray.chunk_size());
        Value v = DKV.get(k2); // we had the last key
        // if we can't get further we might have been the last one and we must
        // commit the latest guy if we had one. 
        if (v == null) {
          if ((state != EXPECT_COND_LF) && (state != POSSIBLE_EMPTY_LINE)) {
            c = CHAR_LF;
            continue MAIN_LOOP;
          }
          break MAIN_LOOP;
        }
        bits = v.get(512);
        if (bits[0] == CHAR_LF && state == EXPECT_COND_LF)
          break MAIN_LOOP; // when the first character we see is a line end
      }
      c = bits[offset];
    } // end MAIN_LOOP
    if (colIdx == 0)
      callback.rollbackLine();
  }

  private static boolean isWhitespace(byte c) {
    return (c == CHAR_SPACE) || (c == CHAR_TAB);
  }

  private static boolean isEOL(byte c) {
    return (c >= CHAR_LF) && ( c<= CHAR_CR);
  }

  private static final byte TOKEN_START = 19;
  private static final byte SECOND_LINE = 20;
  private static final byte SECOND_COND_QUOTED_TOKEN = 21;
  private static final byte SECOND_WHITESPACE_BEFORE_TOKEN = 22;
  private static final byte SECOND_TOKEN_FIRST_LETTER = 23;
  private static final byte SECOND_TOKEN = 24;
  private static final byte SECOND_COND_QUOTE = 25;
  private static final byte SECOND_SEPARATOR_OR_EOL = 26;


  private static boolean canBeInNumber(byte c) {
    return ((c >='0') && ( c <= '9')) || (c == 'E') || (c == 'e') || (c == '.') || (c == '-') || (c == '+');
  }

  @SuppressWarnings("fallthrough")
  public static String [] determineColumnNames(byte[] bits, byte separator) {
    ArrayList<String> colNames = new ArrayList();
    int offset = 0;
    byte state = COND_QUOTED_TOKEN;
    String [] result;
    byte quotes = 0;
    byte c = bits[offset];
    StringBuilder sb = null;
    boolean mightBeNumber = true;
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
          mightBeNumber = true;
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
          mightBeNumber = mightBeNumber && canBeInNumber(c);
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
          if (sb.toString().isEmpty())
            mightBeNumber = false;
          if (mightBeNumber == true)
            return null; // it is a number, so we can't count it as column header
          colNames.add(sb.toString());
          if (isEOL(c)) {
            state = (c == CHAR_CR) ? EXPECT_COND_LF : SECOND_LINE;
            break NEXT_CHAR;
          } else if (c == separator) {
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          } else {
            return null;
          }
        case EXPECT_COND_LF:
          state = SECOND_LINE;
          if (c == CHAR_LF)
            break NEXT_CHAR;
          // fallthrough to SECOND_LINE
        case SECOND_LINE:
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
            state = SECOND_WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          } else {
            return null;
          }
      }
      ++offset;
      if (offset == bits.length)
        return null;
      c = bits[offset];
    }
    if(colNames.isEmpty())return null;
    result = new String[colNames.size()];
    colNames.toArray(result);
    return result;
  }
  
 // Guess type of file (csv comma separated, csv space separated, svmlight) and the number of columns,
 // the number of columns for svm light is not reliable as it only relies on info from the first chunk
 public static int[] guessParserSetup(byte[] b, boolean parseFirst ) {
   // Best-guess on count of columns and separator.  Skip the 1st line.
   // Count column delimiters in the next line. If there are commas, assume file is comma separated.
   // if there are (several) ':', assume it is in svmlight format.

   int i=0;
   // Skip all leading whitespace
   while( i<b.length && Character.isWhitespace(b[i]) ) i++;
   if( !parseFirst ) {         // Skip the first line, it might contain labels
     while( i<b.length && b[i] != '\r' && b[i] != '\n' ) i++; // Skip a line
   }
   if( i+1 < b.length && (b[i] == '\r' && b[i+1]=='\n') ) i++;
   if( i   < b.length &&  b[i] == '\n' ) i++;
   // start counting columns on the 2nd line
   final int line_start = i;
   int cols = 0;
   int mode = 0;
   boolean commas  = false;     // Assume white-space only columns
   boolean escaped = false;
   while( i < b.length ) {
     char c = (char)b[i++];
     if( c == '"' ) {
       escaped = !escaped;
       continue;
     }
     if (!escaped) {
       if( c=='\n' || c== '\r' ) {
         break;
       }
       if( !commas && Character.isWhitespace(c) ) { // Whites-space column seperator
         if( mode == 1 ) mode = 2;
       } else if( c == ',' ) {   // Found a comma?
         if( commas == false ) { // Not in comma-seperator mode?
           // Reset the entire line parse & try again, this time with comma
           // separators enabled.
           commas=true;          // Saw a comma
           i = line_start;       // Reset to line start
           cols = mode = 0;      // Reset parsing mode
           continue;             // Try again
         }
         if( mode == 0 ) cols++;
         mode = 0;
       } else {                  // Else its just column data
         if( mode != 1 ) cols++;
         mode = 1;
       }
     }
   }
   // If no columns, and skipped first row - try again parsing 1st row
   if( cols == 0 && parseFirst == false ) return guessParserSetup(b,true);
   return new int[]{ commas ? CHAR_COMMA : CHAR_SPACE, cols };
 }
  
  
}




