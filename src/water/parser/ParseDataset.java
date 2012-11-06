package water.parser;
import java.io.*;
import java.util.Arrays;
import java.util.zip.*;

import water.*;
import water.ValueArray.Column;

import com.google.common.io.Closeables;

/**¯
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
    long start = System.currentTimeMillis();
    int [] psetup =  guessParserSetup(dataset, false);
    byte sep = (byte)',';
    if(sep == PARSE_SPACESEP)sep = ' ';
    byte [] bits = (dataset instanceof ValueArray)?DKV.get(dataset._key).get(256*1024):dataset.get(256*1024);
    String [] colNames = FastParser.determineColumnNames(bits,sep);
    boolean skipFirstLine = (colNames != null && colNames.length == psetup[1]);
    // pass 1
    DParseTask tsk = new DParseTask(dataset, result, sep,psetup[1],skipFirstLine);
    tsk.invoke(dataset._key);
    long p1end = System.currentTimeMillis() - start;
    tsk = tsk.pass2();
    tsk.invoke(dataset._key);
    // now calculate the column information
    tsk.createValueArrayHeader(colNames,dataset);
    //tsk.check(result);
    start = System.currentTimeMillis() - start;
    System.out.println("Phase 1 took "+p1end);
    System.out.println("Parser took "+start);
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
    // pass 1 types
    static final byte UCOL  = 0;  // unknown
    static final byte ECOL  = 11;  // enum column
    static final byte ICOL  = 12;  // integer column
    static final byte FCOL  = 13;  // float column
    static final byte DCOL  = 14;  // double column
    // pass 2 types
    static final byte BYTE  = 1;
    static final byte SHORT = 2;
    static final byte INT   = 3;
    static final byte LONG  = 4;
    static final byte DSHORT= 5;
    static final byte FLOAT = 6;
    static final byte DOUBLE= 7;
    static final byte STRINGCOL = 8;  // string column (too many enum values)

    static final int [] colSizes = new int[]{0,1,2,4,8,2,-4,-8,0};

    int     _chunkId = -1;

    int     _phase;
    boolean _skipFirstLine;
    int     _myrows;
    int     _ncolumns;
    byte    _sep = (byte)',';
    byte    _decSep = (byte)'.';
    String  _error;
    int     _rpc;
    int     _rowsize;

    transient int [] _outputRows;
    transient int    _outputIdx;
    transient Stream [] _outputStreams;
    Key _resultKey;

    byte     [] _colTypes;
    int      [] _scale;
    long     [] _invalidValues;
    double   [] _min;
    double   [] _max;
    double   [] _mean;
    double   [] _sigma;
    FastTrie [] _enums;
    int      [] _nrows;


    int _numRows; // number of rows -- works only in second pass FIXME in first pass object

    String[][] _colDomains;

    public DParseTask() {}
    public DParseTask(Value dataset, Key resultKey, byte sep, int ncolumns, boolean skipFirstLine) {
      _resultKey = resultKey;
      _ncolumns = ncolumns;
      _sep = sep;
      if (dataset instanceof ValueArray) {
        ValueArray ary = (ValueArray) dataset;
        _nrows = new int[(int)ary.chunks()];
      }

      _skipFirstLine = skipFirstLine;
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
      s.setAry4(_nrows);
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
      _nrows = s.getAry4();
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

    public DParseTask pass2() {
      assert (_phase == 0);
      _colDomains = new String[_ncolumns][];
      for(int i = 0; i < _colTypes.length; ++i){
        if(_colTypes[i] == ECOL){
          _colDomains[i] = _enums[i].compress();
        }
        else _enums[i].kill();
      }
      _bases = new int[_ncolumns];
      calculateColumnEncodings();
      DParseTask tsk = new DParseTask();
      tsk._skipFirstLine = _skipFirstLine;
      tsk._myrows = _myrows; // for simple values, number of rows is kept in the member variable instead of _nrows
      tsk._resultKey = _resultKey;
      tsk._enums = _enums;
      tsk._colTypes = _colTypes;
      tsk._nrows = _nrows;
      tsk._sep = _sep;
      tsk._decSep = _decSep;
      // don't pass invalid values, we do not need them 2nd pass
      tsk._bases = _bases;
      tsk._phase = 1;
      tsk._scale = _scale;
      tsk._ncolumns = _ncolumns;
      tsk._outputRows = _outputRows;
      tsk._colDomains = _colDomains;
      tsk._min = _min;
      tsk._max = _max;
      tsk._mean = _mean;
      tsk._sigma = _sigma;
      if (tsk._nrows != null) {
        _numRows = 0;
        for (int i = 0; i < tsk._nrows.length; ++i) {
          _numRows += tsk._nrows[i];
          tsk._nrows[i] = _numRows;
        }
        tsk._numRows = _numRows;
      } else {
        tsk._numRows = _myrows;
      }
      return tsk;
    }

    public void createValueArrayHeader(String[] colNames,Value dataset) {
      assert (_phase == 1);
      Column[] cols = new Column[_ncolumns];
      int off = 0;
      for(int i = 0; i < cols.length; ++i){
        cols[i]         = new Column();
        cols[i]._badat  = 0; // FIXME (char)Math.min(65535, _invalidValues[i] );
        cols[i]._base   = _bases[i];
        assert (short)pow10i(-_scale[i]) == pow10i(-_scale[i]):"scale out of bounds!,  col = " + i + ", scale = " + _scale[i];
        cols[i]._scale  = (short)pow10i(-_scale[i]);
        cols[i]._off    = (short)off;
        cols[i]._size   = (byte)colSizes[_colTypes[i]];
        cols[i]._domain = new ValueArray.ColumnDomain(_colDomains[i]);
        cols[i]._max    = _max[i];
        cols[i]._min    = _min[i];
        cols[i]._mean   = 0; // FIXME _mean[i];
        cols[i]._sigma  = 0; // FIXME tsk._sigma[i];
        cols[i]._name   =  colNames == null ? String.valueOf(i) : colNames[i];
        off +=  Math.abs(cols[i]._size);
      }
      // finally make the value array header
      ValueArray ary = ValueArray.make(_resultKey, Value.ICE, dataset._key, "basic_parse", _numRows, off, cols);
      DKV.put(_resultKey, ary);
    }

    // DO NOT THROW AWAY THIS CODE, I WILL USE IT IN VABUILDER AFTER WE MERGE!!!!!!!!
    // (function check)
    void check(Key k) {
      assert (k==_resultKey);
      System.out.println(_numRows);
      Value v = DKV.get(k);
      assert (v != null);
      assert (v instanceof ValueArray);
      ValueArray va = (ValueArray) v;
      System.out.println("Num rows:     "+va.num_rows());
      System.out.println("Num cols:     "+va.num_cols());
      System.out.println("Rowsize:      "+va.row_size());
      System.out.println("Length:       "+va.length());
      System.out.println("Rows:         "+((double)va.length() / va.row_size()));
      assert (va.num_rows() == va.length() / va.row_size());
      System.out.println("Chunk size:   "+(ValueArray.chunk_size() / va.row_size()) * va.row_size());
      System.out.println("RPC:          "+ValueArray.chunk_size() / va.row_size());
      System.out.println("Num chunks:   "+va.chunks());
      long totalSize = 0;
      long totalRows = 0;
      for (int i = 0; i < va.chunks(); ++i) {
        System.out.println("  chunk:             "+i);
        System.out.println("    chunk off:         "+ValueArray.chunk_offset(i)+" (reported by VA)");
        System.out.println("    chunk real off:    "+i * ValueArray.chunk_size() / va.row_size() * va.row_size());
        Value c = DKV.get(va.chunk_get(i));
        if (c == null)
          System.out.println("                       CHUNK AS REPORTED BY VA NOT FOUND");
        assert (c!=null):"missing chunk " + i;
        System.out.println("    chunk size:        "+c.length());
        System.out.println("    chunk rows:        "+c.length() / va.row_size());
        byte[] b = c.get();
        assert (b.length == c.length());
        totalSize += c.length();
        System.out.println("    total size:        "+totalSize);
        totalRows += c.length() / va.row_size();
        System.out.println("    total rows:        "+totalRows);
      }
      System.out.println("Length exp:   "+va.length());
      System.out.println("Length:       "+totalSize);
      System.out.println("Rows exp:     "+((double)va.length() / va.row_size()));
      System.out.println("Rows:         "+totalRows);
      assert (totalSize == va.length()):"totalSize: " + totalSize + ", va.length(): " + va.length();
      assert (totalRows == ((double)va.length() / va.row_size()));
    }


    @Override public void write( DataOutputStream dos ) throws IOException {
      dos.writeInt(_phase);
      dos.writeInt(_ncolumns);
      dos.writeByte(_sep);
      dos.writeByte(_decSep);
      if(_nrows != null){
        dos.write(_nrows.length);
        for(int i:_nrows)dos.writeInt(i);
      } else dos.write(-1);

    }

    @Override public void read ( DataInputStream dis  ) throws IOException {
      _phase = dis.readInt();
      _ncolumns = dis.readInt();
      _sep = dis.readByte();
      _decSep = dis.readByte();
      int n = dis.readInt();
      if(n != -1){
        _nrows = new int[n];
        for(int i = 0; i < n; ++i)_nrows[i] = dis.readInt();
      }

    }



    @Override
    public void map(Key key) {
      try{
        Key aryKey = null;
        boolean arraylet = key._kb[0] == Key.ARRAYLET_CHUNK;
        boolean skipFirstLine = _skipFirstLine;
        if(arraylet) {
          aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
          _chunkId = ValueArray.getChunkIndex(key);
          skipFirstLine = skipFirstLine || (ValueArray.getChunkIndex(key) != 0);
          if(_phase == 0){
            ValueArray ary = (ValueArray)DKV.get(aryKey);
          }
        }
        switch(_phase){
        case 0:
          _enums = new FastTrie[_ncolumns];
          _invalidValues = new long[_ncolumns];
          _min = new double [_ncolumns];
          Arrays.fill(_min, Double.MAX_VALUE);
          _max = new double[_ncolumns];
          Arrays.fill(_max, Double.MIN_VALUE);
          _mean = new double[_ncolumns];
          _scale = new int[_ncolumns];
          for(int i = 0; i < _enums.length; ++i)_enums[i] = new FastTrie();
          _colTypes = new byte[_ncolumns];
          FastParser p = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
          p.parse(key,skipFirstLine);
          if(arraylet) {
            assert (_nrows[ValueArray.getChunkIndex(key)] == 0);
            _nrows[ValueArray.getChunkIndex(key)] = _myrows;
          }

          break;
        case 1:
          _sigma = new double[_ncolumns];
          int rowsize = 0;
          for(byte b:_colTypes)rowsize += Math.abs(colSizes[b]);
          int rpc = (int)ValueArray.chunk_size()/rowsize;
          int firstRow = 0;
          int lastRow = _myrows;
          _myrows = 0;
          if(arraylet){
            long origChunkIdx = ValueArray.getChunkIndex(key);
            firstRow = (origChunkIdx == 0) ? 0 : _nrows[(int)origChunkIdx-1];
            lastRow = _nrows[(int)origChunkIdx];
          }
          int rowsToParse = lastRow - firstRow;
          int chunkRows = Math.min(rpc - (firstRow % rpc),rowsToParse);
          int n = 1;
          while (chunkRows + ((n-1)*rpc) < rowsToParse) ++n;
          _outputRows = new int[n];
          _outputStreams = new Stream[n];
          int j = 0;
          while (rowsToParse > 0) {
            _outputRows[j] = chunkRows;
            _outputStreams[j] = new Stream(chunkRows*rowsize);
            rowsToParse -= chunkRows;
            chunkRows = Math.min(rowsToParse,rpc);
            ++j;
          }
          _s = _outputStreams[0];
          FastParser p2 = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
          p2.parse(key,skipFirstLine);
          int inChunkOffset = (firstRow % rpc) * rowsize; // index into the chunk I am writing to
          int lastChunk = Math.max(1,this._numRows / rpc) - 1; // index of the last chunk in the VA
          int chunkIndex = firstRow/rpc; // index of the chunk I am writing to
          if (chunkIndex > lastChunk) {
            assert (chunkIndex == lastChunk + 1);
            inChunkOffset += rpc * rowsize;
            --chunkIndex;
          }
          for (int i = 0; i < _outputStreams.length; ++i) {
            Key k = ValueArray.make_chunkkey(_resultKey,ValueArray.chunk_offset(chunkIndex));
            //assert (_outputStreams[i]._off == _outputStreams[i]._buf.length);
            AtomicUnion u = new AtomicUnion(_outputStreams[i]._buf,0,inChunkOffset,_outputStreams[i]._buf.length);
            lazy_complete(u.fork(k));
            if (chunkIndex == lastChunk) {
              inChunkOffset += _outputStreams[i]._buf.length;
            } else {
              ++chunkIndex;
              inChunkOffset = 0;
            }
          }
          break;
        default:
          assert false:"unexpected phase " + _phase;
        }
      }catch(Exception e){
        e.printStackTrace();
        _error = e.getMessage();
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      DParseTask other = (DParseTask)drt;
      if(_sigma == null)_sigma = other._sigma;
      if(_enums == null){
        assert _min == null;
        assert _max == null;
        assert _scale == null;
        assert _colTypes == null;
        _enums = other._enums;
        _min = other._min;
        _max = other._max;
        _scale = other._scale;
        _colTypes = other._colTypes;
        _nrows = other._nrows;
      } else {
        if (_phase == 0) {
          if (_nrows != other._nrows)
            for (int i = 0; i < _nrows.length; ++i)
              _nrows[i] += other._nrows[i];
          for(int i = 0; i < _ncolumns; ++i) {
            _enums[i].merge(other._enums[i]);
            if(other._min[i] < _min[i])_min[i] = other._min[i];
            if(other._max[i] > _max[i])_max[i] = other._max[i];
            if(other._scale[i] > _scale[i])_scale[i] = other._scale[i];
            if(other._colTypes[i] > _colTypes[i])_colTypes[i] = other._colTypes[i];
          }
        } else {
          // pass -- phase 1 does not require any reduction of these
        }
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
      1.0,
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
      1,
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
      assert 10 >= exp && exp >= 0:"unexpceted exponent " + exp;
      return powers10i[exp];
    }

    int [] _bases;
    Stream _s;

    private void calculateColumnEncodings(){
      assert (_bases != null);
      assert (_min != null);
      for(int i = 0; i < _ncolumns; ++i){
        switch(_colTypes[i]){
        case ICOL: // number
          if (_max[i] - _min[i] < 255) {
            _colTypes[i] = BYTE;
            _bases[i] = (int)_min[i];
          } else if ((_max[i] - _min[i]) < 65535) {
            _colTypes[i] = SHORT;
            _bases[i] = (int)_min[i];
          } else if (_max[i] - _min[i] < (1l << 32)) {
              _colTypes[i] = INT;
            _bases[i] = (int)_min[i];
          } else
            _colTypes[i] = LONG;
          break;
        case FCOL:
        case DCOL:
          double s = pow10(-_scale[i]);
          double range = s*(_max[i]-_min[i]);
          if(range < 65535){
            _colTypes[i] = DSHORT;
            _bases[i] = (int)(s*_min[i]);
          } else {
            _scale[i] = 0;
            _bases[i] = 0;
            _colTypes[i] = (_colTypes[i] == FCOL)?FLOAT:DOUBLE;
          }
          break;
        case ECOL: // enum
          _max[i] = _enums[i]._state0-1;
          _min[i] = 0;
          if(_enums[i]._state0 < 256)_colTypes[i] = BYTE;
          else if(_enums[i]._state0 < 65536)_colTypes[i] = SHORT;
          else _colTypes[i] = INT;
        }
      }
    }


    public void addRow(FastParser.Row row) {
      ++_myrows;
      switch (_phase) {
      case 0:
        for(int i = 0; i < _ncolumns; ++i){
          switch(row._numLength[i]) {
          case -1:
            continue; //NaN
          case -2:
            if(_colTypes[i] ==UCOL)_colTypes[i] = ECOL;
            break;
           default:
             assert row._numLength[i] >= 0:"unexpected num length " + row._numLength[i];
             double d = row._numbers[i]*pow10(row._exponents[i]);
             if(d < _min[i])_min[i] = d;
             if(d > _max[i])_max[i] = d;
             if(row._exponents[i] < _scale[i]) {
               _scale[i] = row._exponents[i];
               if(_colTypes[i] != DCOL){
                 if((float)d != d)_colTypes[i] = DCOL;
                 else _colTypes[i] = FCOL;
               }
             } else if(_colTypes[i] < ICOL)
              _colTypes[i] = ICOL;
          }
        }
        break;
      case 1:
        if(_myrows > _outputRows[_outputIdx]) {
          ++_outputIdx;
          assert (_outputIdx < _outputStreams.length);
          _s = _outputStreams[_outputIdx];
          _myrows = 1;
        }
        for (int i = 0; i < row._numbers.length; ++i) {
          switch(row._numLength[i]) {
          case -1: // NaN
            row._numbers[i]  = -1l;
            row._numbers[i] += _bases[i];
            // fallthrough -1 is NaN for all values, _lbases will cancel each other
            // -1 is also NaN in case of enum (we're in number column)
          case -2: // enum
            // lbase for enums is 0
          default:
            switch (_colTypes[i]) {
              case BYTE:
                _s.set1((byte)(row._numbers[i]*pow10i(row._exponents[i] - _scale[i]) - _bases[i]));
                break;
              case SHORT:
                _s.set2((short)(row._numbers[i]*pow10i(row._exponents[i] - _scale[i]) - _bases[i]));
                break;
              case INT:
                _s.set4((int)(row._numbers[i]*pow10i(row._exponents[i] - _scale[i]) - _bases[i]));
                break;
              case LONG:
                _s.set8(row._numbers[i]*pow10i(row._exponents[i] - _scale[i]));
                break;
              case FLOAT:
                _s.set4f((float)(row._numbers[i] * pow10(row._exponents[i])));
                break;
              case DOUBLE:
                _s.set8d(row._numbers[i] * pow10(row._exponents[i]));
                break;
              case DSHORT:
                // scale is computed as negative in the first pass,
                // therefore to compute the positive exponent after scale, we add scale and the original exponent
                _s.set2((short)(row._numbers[i]*pow10i(row._exponents[i] - _scale[i]) - _bases[i]));
                break;
            }
          }
        }
        break;
      default:
        assert false:"unexpected phase " + _phase;
      }
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




