package water.parser;
import java.io.*;
import java.util.*;
import java.util.zip.*;

import water.*;
import water.parser.SeparatedValueParser.Row;

import com.google.common.io.Closeables;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public final class ParseDataset {
  static enum Compression { NONE, ZIP, GZIP }


//Guess
 private static Compression guessCompressionMethod(Value dataset) {
   Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
   byte[] b = v0.get();                      // Bytes for 1st chunk

   // Look for ZIP magic
   if (b.length > ZipFile.LOCHDR && UDP.get4(b, 0) == ZipFile.LOCSIG)
     return Compression.ZIP;
   if (b.length > 2 && UDP.get2(b, 0) == GZIPInputStream.GZIP_MAGIC)
     return Compression.GZIP;
   return Compression.NONE;
 }


//---
 // Guess type of file (csv comma separated, csv space separated, svmlight) and the number of columns,
 // the number of columns for svm light is not reliable as it only relies on info from the first chunk
 private static int[] guessParserSetup(Value dataset, boolean parseFirst ) {
   // Best-guess on count of columns and separator.  Skip the 1st line.
   // Count column delimiters in the next line. If there are commas, assume file is comma separated.
   // if there are (several) ':', assume it is in svmlight format.
   Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
   byte[] b = v0.get();                      // Bytes for 1st chunk

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
   int colonCounter = 0;
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
       } else if(c == ':' && (++colonCounter == 3)){
         // if there are at least 3 ':' on the line, the file is probably svmlight format
         throw new Error("SVMLIGHT format is currently unsupported");
       } else {                  // Else its just column data
         if( mode != 1 ) cols++;
         mode = 1;
       }
     }
   }
   // If no columns, and skipped first row - try again parsing 1st row
   if( cols == 0 && parseFirst == false ) return guessParserSetup(dataset,true);
   return new int[]{ commas ? PARSE_COMMASEP : PARSE_SPACESEP, cols };
 }

 // ---

 // Alternative column title guesser.  Returns an array of Strings, or
 // null if none.
 public static String[] guessColNames( Value dataset, int [] psetup ) {
   return null;
 }

  // Configuration kind for parser
  public static final int PARSE_COMMASEP = 102;
  private static final int PARSE_SPACESEP = 103;

  // Index to array returned by method guesss_parser_setup()
  static final int PARSER_IDX = 0;
  static final int COLNUM_IDX = 1;

  // Parse the dataset (uncompressed, zippped) as a CSV-style thingy and produce a structured dataset as a
  // result.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");
    try {
      Compression compression = guessCompressionMethod(dataset);
      switch (compression) {
      case NONE: parseUncompressed(result, dataset); break;
      case ZIP : parseZipped      (result, dataset); break;
      case GZIP: parseGZipped     (result, dataset); break;
      default  : throw new Error("Uknown compression of dataset!");
      }
    } catch( IOException e ) {
      throw new Error(e);
    }
  }


 // Parse the uncompressed dataset as a CSV-style structure and produce a structured dataset
 // result.  This does a distributed parallel parse.
  public static void parseUncompressed( Key result, Value dataset ) throws IOException {
    // Guess on the number of columns, build a column array.
    int [] psetup =  guessParserSetup(dataset, false);
    String [] colNames = guessColNames(dataset,psetup);
    boolean skipFirstLine = (colNames != null && colNames.length == psetup[1]);
    // pass 1
    DParseTask tsk = new DParseTask(psetup[0],psetup[1]);
  }


  // Unpack zipped CSV-style structure and call method parseUncompressed(...)
  // The method exepct a dataset which contains a ZIP file encapsulating one file.
  public static void parseZipped( Key result, Value dataset ) throws IOException {
    // Dataset contains zipped CSV
    ZipInputStream zis = null;
    Key key = null;
    try {
      // Create Zip input stream and uncompress the data into a new key <ORIGINAL-KEY-NAME>_UNZIPPED
      zis = new ZipInputStream(dataset.openStream());
      // Get the *FIRST* entry
      ZipEntry ze = zis.getNextEntry();
      // There is at least one entry in zip file and it is not a directory.
      if (ze != null && !ze.isDirectory()) {
        key = ValueArray.read_put_stream(new String(dataset._key._kb) + "_UNZIPPED", zis, Key.DEFAULT_DESIRED_REPLICA_FACTOR);
      }
      // else it is possible to dive into a directory but in this case I would
      // prefer to return error since the ZIP file has not expected format
    } finally { Closeables.closeQuietly(zis); }

    if( key == null ) throw new Error("Cannot uncompressed ZIP-compressed dataset!");

    Value uncompressedDataset = DKV.get(key);
    parse(result, uncompressedDataset);
  }

  public static void parseGZipped( Key result, Value dataset ) throws IOException {
    GZIPInputStream gzis = null;
    Key key = null;
    try {
      gzis = new GZIPInputStream(dataset.openStream());
      key = ValueArray.read_put_stream(new String(dataset._key._kb) + "_UNZIPPED", gzis, Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    } finally { Closeables.closeQuietly(gzis); }

    if( key == null ) throw new Error("Cannot uncompressed GZIP-compressed dataset!");
    Value uncompressedDataset = DKV.get(key);
    parse(result, uncompressedDataset);
  }

  // True if the array is all NaNs
  final static boolean allNaNs( double ds[] ) {
    for( double d : ds )
      if( !Double.isNaN(d) )
        return false;
    return true;
  }

  public static final class DParseTask extends MRTask {
    static final byte SCOL = -3;  // string column (too many enum values)
    // pass 1 types
    static final byte UCOL  = 0;  // unknown
    static final byte ECOL  = 1;  // enum column
    static final byte ICOL  = 2;  // integer column
    static final byte FCOL  = 3;  // float column
    static final byte DCOL  = 4;  // double column
    // pass 2 types
    static final byte BYTE  = 5;
    static final byte SHORT = 6;
    static final byte INT   = 7;
    static final byte LONG  = 8;
    static final byte DSHORT= 9;

    int _phase;
    int  _myrows;
    int _ncolumns;
    byte _sep = (byte)',';
    byte _decSep = (byte)'.';

    String _error;
    byte     [] _colTypes;
    int      [] _scale;
    long     [] _invalidValues;
    double   [] _min;
    double   [] _max;
    FastTrie [] _enums;
    int      [] _nrows;


    public DParseTask() {}
    public DParseTask(Value dataset, byte sep, int ncolumns) {
      _ncolumns = ncolumns;
      _sep = sep;
    }
    @Override public int wire_len() {
      switch(_phase){
      case 0:
        return 4 + 4 + 1 + 1;
      }
      return 0;
      }

    @Override public void write( Stream s ) {
      s.set4(_phase);
      s.set4(_ncolumns);
      s.set1(_sep);
      s.set1(_decSep);
      s.setAry1(_colTypes);
      s.setAry4(_scale);
      s.setAry8(_invalidValues);
      s.setAry8d(_min);
      s.setAry8d(_max);
      if(_enums != null){
        s.set4(_enums.length);
        for(FastTrie t:_enums) t.write(s);
      } else
        s.set4(-1);
    }
    @Override public void read ( Stream s ) {
      _phase = s.get4();
      _ncolumns = s.get4();
      _sep = s.get1();
      _decSep = s.get1();
      _colTypes = s.getAry1();
      _scale = s.getAry4();
      _invalidValues = s.getAry8();
      _min = s.getAry8d();
      _max = s.getAry8d();
      int n = s.get4();
      if(n != -1){
        _enums = new FastTrie[n];
        for(int i = 0; i < n; ++i){
          _enums[i] = new FastTrie();
          _enums[i].read(s);
        }
      }
    }

    @Override public void write( DataOutputStream dos ) throws IOException {
      dos.writeInt(_phase);
      dos.writeInt(_ncolumns);
      dos.writeByte(_sep);
      dos.writeByte(_decSep);
      switch(_phase){
      case 0:
        break;
      default:
        throw new Error("illegal phase " + _phase);
      }
    }

    @Override public void read ( DataInputStream dis  ) throws IOException {
      _phase = dis.readInt();
      _ncolumns = dis.readInt();
      _sep = dis.readByte();
      _decSep = dis.readByte();
      switch(_phase){
      case 0:
        break;
      case 1:
        break;
      default:
        throw new Error("illegal phase " + _phase);
      }
    }



    @Override
    public void map(Key key) {
      Key aryKey  = Key.make(ValueArray.getArrayKeyBytes(key));

      if(_phase == 1)_colTypes = new byte[_ncolumns];
      else for(byte b:_colTypes){

      }
      try{
        FastParser p = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
        p.parse(key,true);
        ++_phase;
      }catch(Exception e){
        _error = e.getMessage();
      }
      _nrows[ValueArray.getChunkIndex(key)] = _myrows;
    }

    @Override
    public void reduce(DRemoteTask drt) {
      DParseTask other = (DParseTask)drt;
      for(int i = 0; i < _nrows.length; ++i){
        assert _nrows[i] == 0 || other._nrows[i] == 0;
        _nrows[i] += other._nrows[i];
      }
      if(_enums == null){
        _enums = other._enums;
        assert _min == null;
        _min = other._min;
        assert _max == null;
        _max = other._max;
        assert _scale == null;
        _scale = other._scale;
        assert _colTypes == null;
        _colTypes = other._colTypes;
      } else for(int i = 0; i < _ncolumns; ++i) {
        _enums[i].merge(other._enums[i]);
        if(other._min[i] < _min[i])_min[i] = other._min[i];
        if(other._max[i] > _max[i])_max[i] = other._max[i];
        if(other._scale[i] > _scale[i])_scale[i] = other._scale[i];
        if(other._colTypes[i] > _colTypes[i])_colTypes[i] = other._colTypes[i];
      }
      if(_error == null)_error = other._error;
      else if(other._error != null) _error = _error + "\n" + other._error;
    }

    static double [] powers10 = new double[]{
      0.0000000001,
      0.000000001,
      0.00000001,
      0.0000001,
      0.000001,
      0.00001,
      0.0001,
      0.001,
      0.01,
      0.1,
      0.0,
      10.0,
      100.0,
      1000.0,
      10000.0,
      100000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      10000000000.0
    };

    static long [] powers10i = new long[]{
      0,
      10,
      100,
      1000,
      10000,
      100000,
      1000000,
      10000000,
      100000000,
      1000000000,
      10000000000l
    };

    static double pow10(int exp){
      return ((exp >= -10 && exp <= 10)?powers10[exp+10]:Math.pow(10, exp));
    }

    static long pow10i(int exp){
      assert 10 >= exp && exp >= 0;
      return powers10i[exp];
    }

    double [] _dbases;
    long   [] _lbases;
    Stream _s;

    private void calculateColumnEncodings(){
      for(int i = 0; i < _ncolumns; ++i){
        switch(_colTypes[i]){
        case ICOL: // number
          if (_max[i] - _min[i] < 255) {
            _colTypes[i] = BYTE;
            _lbases[i] = (long)_min[i];
          } else if ((_max[i] - _min[i]) < 65535) {
            _colTypes[i] = SHORT;
            _lbases[i] = (long)_min[i];
          } else if (_max[i] - _min[i] < (1l << 32)) {
              _colTypes[i] = INT;
            _lbases[i] = (long)_min[i];
          } else
            _colTypes[i] = LONG;
          break;
        case FCOL:
        case DCOL:
          double s = pow10(_scale[i]);
          double range = s*(_max[i]-_min[i]);
          if(range < 65535){
            _colTypes[i] = DSHORT;
            _lbases[i] = (long)(s*_min[i]);
          } // else leave it as float/double
        case ECOL: // enum
          if(_enums[i]._nfinalStates < 256)_colTypes[i] = BYTE;
          else if(_enums[i]._nfinalStates < 65536)_colTypes[i] = SHORT;
          else _colTypes[i] = INT;
        }
      }
    }

    public void addRow(FastParser.Row row){
      switch (_phase) {
      case 0:
        ++_myrows;
        for(int i = 0; i < _ncolumns; ++i){
          if(row._numLength[i] < 0)++_invalidValues[i];
          if(row._numbers[i] == -1)continue; //NaN
          if(row._numbers[i] == -2){
            // enum
            switch(_colTypes[i]){
            case UCOL:
              _colTypes[i] = ECOL;
            case ECOL:
              if(row._numbers[i] == -1)_colTypes[i] = SCOL;
              break;
            default:
              break;
            }
          } else { // number
            double d = row._numbers[i]*pow10(row._exponents[i]);
            if(d < _min[i])_min[i] = d;
            if(d > _max[i])_max[i] = d;
            int exp = row._numLength[i] + row._exponents[i];
            if(exp < 0) {
              if(exp < _scale[i])_scale[i] = exp;
              if(_colTypes[i] != DCOL){
                if((float)d != d)_colTypes[i] = DCOL;
                else _colTypes[i] = FCOL;
              }
            } else if(_colTypes[i] == UCOL || _colTypes[i] == ECOL)
              _colTypes[i] = ICOL;
          }
        }
      case 2:
        for (int i = 0; i < row._numbers.length; ++i) {
          switch(row._numLength[i]){
          case -1: // NaN
            row._numbers[i] = -1l;
            row._numbers[i] += _lbases[i];
            // fallthrough -1 is NaN for all values, _lbases will cancel each other
            // -1 is also NaN in case of enum (we're in number column)
          case -2: // enum
            // lbase for enums is 0
          default:
            switch (_colTypes[i]) {
              case BYTE:
                _s.set1((byte)(row._numbers[i] - _lbases[i]));
                break;
              case SHORT:
                _s.set2((short)(row._numbers[i] - _lbases[i]));
                break;
              case INT:
                _s.set4((int)(row._numbers[i] - _lbases[i]));
                break;
              case LONG:
                _s.set8(row._numbers[i]);
                break;
              case FCOL:
                _s.set4f((float)(row._numbers[i] * pow10(row._exponents[i])));
                break;
              case DCOL:
                _s.set8d(row._numbers[i] * pow10(row._exponents[i]));
              case DSHORT:
                _s.set2((short)(row._numbers[i]*pow10i(_scale[i]+row._exponents[i]) - _lbases[i]));
                break;
            }
          }
        }
      }
    }
  }
  // ----
  // Distributed parsing.


  // Atomically fold together as many rows as will fit in the next chunk.  I
  // have an array of bits (buf) which is an even count of rows.  I want to
  // pack them into the target ValueArray, as many as will fit in a next
  // chunk.  Because the size isn't an even multiple of chunks, I surely will
  // need to update multiple target chunks.  (imagine parallel copying a
  // large source buffer into a chunked target buffer)
  public static void packRowsIntoValueArrayChunks( Key result, int startRow, int numRows,
                                                   int rowSize, ParseState state, byte[] buf, DRemoteTask drt) {

    // Compute the last dst chunk (which might be up to 2meg instead of capped at 1meg)
    int maxRow = state._rows_chk[state._rows_chk.length-1];
    int rowsPerChunk = (int)(ValueArray.chunk_size()/rowSize);
    int maxChunk = maxRow/rowsPerChunk;

    // Now, rather painfully, ship the bits to the target keys.  Ship in
    // large chunks according to what fits in the next target chunk.
    int curRow = 0;             // Number of processed rows
    while( curRow < numRows ) {
      int rowToWrite = startRow+curRow;  // First row to write to
      int chunk = rowToWrite/rowsPerChunk;        // First chunk to write to
      if( chunk > 0 && chunk == maxChunk ) // Last chunk?
        chunk--;                 // It's actually the prior chunk, made bigger
      // Get the key for that chunk.  Note that this key may not yet exist.
      Key key = ValueArray.make_chunkkey(result,ValueArray.chunk_offset(chunk));
      // Get the starting row# for this chunk
      int rowsAlreadyWritten = chunk*rowsPerChunk;
      // Get the number of rows to skip
      int firstRowInChunk = rowToWrite-rowsAlreadyWritten;
      int dstOff = firstRowInChunk*rowSize; // Offset in the dest chunk
      int srcOff = curRow*rowSize; // Offset in buf to read from

      // Rows to write in this chunk
      int rowy = rowsPerChunk - firstRowInChunk;      // Number of rows we could write in a 1meg chunk
      int rowz = numRows - curRow; // Number of unwritten rows in our source
      if( chunk < maxChunk-1 && rowz > rowy ) // Not last chunk (which is large) and more rows
        rowz = rowy;              // Limit of rows to write
      int len = rowz*rowSize;    // Bytes to write

      // Remotely, atomically, merge this buffer into the remote key
      assert srcOff+len <= buf.length;
      AtomicUnion au = new AtomicUnion(buf, srcOff, dstOff, len);
      DFutureTask autask = au.fork(key);
      if( drt != null ) drt.lazy_complete(autask); // Start atomic update
      // Do not wait on completion now; the atomic-update is fire-and-forget here.
      curRow += rowz;              // Rows written out
    }
  }

  public static class AtomicUnion extends Atomic {
    Key _key;
    int _dst_off;

    public AtomicUnion() {}
    public AtomicUnion(byte [] buf, int srcOff, int dstOff, int len){
      _dst_off = dstOff;
      _key = Key.make(Key.make()._kb, (byte) 1, Key.DFJ_INTERNAL_USER, H2O.SELF);
      DKV.put(_key, new Value(_key, MemoryManager.arrayCopyOfRange(buf, srcOff, srcOff+len)));
    }

    @Override public byte[] atomic( byte[] bits1 ) {
      byte[] mem = DKV.get(_key).get();
      int len = Math.max(_dst_off + mem.length,bits1==null?0:bits1.length);
      byte[] bits2 = MemoryManager.allocateMemory(len);
      if( bits1 != null ) System.arraycopy(bits1,0,bits2,0,bits1.length);
      System.arraycopy(mem,0,bits2,_dst_off,mem.length);
      return bits2;
    }

    @Override public void onSuccess() {
      DKV.remove(_key);
    }
  }
}




