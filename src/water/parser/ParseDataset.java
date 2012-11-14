package water.parser;
import init.H2OSerializable;

import java.io.IOException;
import java.util.Arrays;
import java.util.zip.*;

import water.*;
import water.ValueArray.Column;
import water.nbhm.NonBlockingHashMap;

import com.google.common.io.Closeables;

/**�
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
@SuppressWarnings("fallthrough")
public final class ParseDataset {
  static enum Compression { NONE, ZIP, GZIP }

  private static final int PHASE_ONE = 0;
  private static final int PHASE_TWO = 1;

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


 public final static class ValueString implements CharSequence {
   byte [] _buff;
   int _off;
   int _length;

   @Override
   public int hashCode(){
     int hash = 1;
     int n = _off + _length;
     for (int i = _off; i < n; ++i)
       hash = 31 * hash + _buff[i];
     return hash;
   }

  @Override
  public char charAt(int index) {
    return (char)_buff[_off+index];
  }

  @Override
  public int length() {
    return _length;
  }
  @Override
  public CharSequence subSequence(int start, int end) {
    return new String(_buff,_off+start, end-start);
  }
  @Override
  public String toString(){
    return new String(_buff,_off,_length);
  }
  
  public ValueString() { }
  
  public ValueString(String from) {
    _buff = from.getBytes();
    _off = 0;
    _length = _buff.length;
  }
  
  public ValueString setTo(String what) {
    _buff = what.getBytes();
    _off = 0;
    _length = _buff.length;
    return this;
  }
 }




 int getTokenId(int colIdx){
   return -1;
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
   if( cols == 0 && parseFirst == false ) return guessParserSetup(dataset,true);
   return new int[]{ commas ? PARSE_COMMASEP : PARSE_SPACESEP, cols };
 }


  // Configuration kind for parser
  public static final int PARSE_COMMASEP = 102;
  private static final int PARSE_SPACESEP = 103;

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
    } catch( Exception e ) {
      throw new Error(e);
    }
  }


 // Parse the uncompressed dataset as a CSV-style structure and produce a structured dataset
 // result.  This does a distributed parallel parse.
  public static void parseUncompressed( Key result, Value dataset ) throws IOException {
    String datasetName = new String(dataset._key._kb);
    if(datasetName.endsWith(".xls") || datasetName.contains(".xls."))
      throw new Error("xls format is currently not supported.");
    // Guess on the number of columns, build a column array.
    int [] psetup =  guessParserSetup(dataset, false);
    byte sep = (byte)',';
    if(sep == PARSE_SPACESEP)sep = ' ';
    byte [] bits = (dataset instanceof ValueArray) ? DKV.get(((ValueArray)dataset).make_chunkkey(0)).get(256*1024) : dataset.get(256*1024);
    String [] colNames = FastParser.determineColumnNames(bits,sep);
    boolean skipFirstLine = colNames != null;
    if (colNames!=null) {
      psetup[1] = colNames.length;
      // TODO Parser setup is aparently not working properly
    }
    // pass 1
    DParseTask tsk = new DParseTask(dataset, result, sep,psetup[1],skipFirstLine);
    tsk.invoke(dataset._key);
    tsk = tsk.pass2();
    tsk.invoke(dataset._key);
    // normalize sigma
    for(int i = 0; i < tsk._ncolumns; ++i)
      tsk._sigma[i] = Math.sqrt(tsk._sigma[i]/(tsk._numRows - tsk._invalidValues[i]));
    tsk.createValueArrayHeader(colNames,dataset);
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
  static boolean allNaNs( double ds[] ) {
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

    static final int [] colSizes = new int[]{0,1,2,4,8,2,-4,-8,1};

    // scalar variables
    boolean _skipFirstLine;
    int     _chunkId = -1;
    int     _phase;
    int     _myrows;
    int     _ncolumns;
    byte    _sep = (byte)',';
    byte    _decSep = (byte)'.';
    int     _rpc;
    int     _rowsize;
    int     _numRows; // number of rows -- works only in second pass FIXME in first pass object
    // 31 bytes

    Key _resultKey;
    String  _error;


    final class Enum implements H2OSerializable {
      NonBlockingHashMap<CharSequence, Integer> _map;

      Enum(){
        _map = new NonBlockingHashMap<CharSequence, Integer>();
      }

      int getTokenId(CharSequence str){


        return -1;
      }

      public void merge(Enum other){
        if(this != other) {
          NonBlockingHashMap<CharSequence, Integer> myMap = _map;
          for(CharSequence str:other._map.keySet()){
            myMap.put(str, 0);
          }
        }
      }
      public int size() {return _map.size();}
      public boolean isKilled() {return _map == null;}
      public void kill(){_map = null;}

      public String [] computeColumnDomain(){
        String [] res = new String[_map.size()];
        NonBlockingHashMap<CharSequence, Integer> oldMap = _map;
        if(oldMap == null)return null;
        oldMap.keySet().toArray(res);
        Arrays.sort(res);
        NonBlockingHashMap<CharSequence, Integer> newMap = new NonBlockingHashMap<CharSequence, Integer>();
        for(int j = 0; j < res.length; ++j)
          newMap.put(res[j], j);
        oldMap.clear();
        _map = newMap;
        return res;
      }

    }


    // arrays
    byte     [] _colTypes;
    int      [] _scale;
    int      [] _bases;
    long     [] _invalidValues;
    double   [] _min;
    double   [] _max;
    double   [] _mean;
    double   [] _sigma;
    int      [] _nrows;
    Enum     [] _enums;


    // transients - each map creates and uses it's own, no need to get these back
    transient int [] _outputRows;
    transient int    _outputIdx;
    transient Stream [] _outputStreams;
    // create and used only on the task caller's side
    transient String[][] _colDomains;

    transient int _lastOffset;

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
      _enums = new Enum[_ncolumns];
      for(int i = 0; i < _ncolumns; ++i)
        _enums[i] = new Enum();
    }

    public DParseTask pass2() {
      assert (_phase == 0);
      _colDomains = new String[_ncolumns][];
      for(int i = 0; i < _colTypes.length; ++i){
        if(_colTypes[i] == ECOL && _enums[i] != null && !_enums[i].isKilled())
          _colDomains[i] = _enums[i].computeColumnDomain();
        else
          _enums[i] = null;
      }
      _bases = new int[_ncolumns];
      calculateColumnEncodings();
      if (_nrows != null) {
        _numRows = 0;
        for (int i = 0; i < _nrows.length; ++i) {
          _numRows += _nrows[i];
          _nrows[i] = _numRows;
        }
      } else {
        _numRows = _myrows;
      }
      // normalize mean
      for(int i = 0; i < _ncolumns; ++i)
        _mean[i] = _mean[i]/(_numRows - _invalidValues[i]);
      DParseTask tsk = new DParseTask();
      tsk._skipFirstLine = _skipFirstLine;
      tsk._myrows = _myrows; // for simple values, number of rows is kept in the member variable instead of _nrows
      tsk._resultKey = _resultKey;
      tsk._enums = _enums;
      tsk._colTypes = _colTypes;
      tsk._nrows = _nrows;
      tsk._numRows = _numRows;
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
      return tsk;
    }

    public void createValueArrayHeader(String[] colNames,Value dataset) {
      assert (_phase == 1);
      Column[] cols = new Column[_ncolumns];
      int off = 0;
      for(int i = 0; i < cols.length; ++i){
        cols[i]         = new Column();
        cols[i]._badat  = (char)Math.min(65535, _invalidValues[i] );
        cols[i]._base   = _bases[i];
        assert (short)pow10i(-_scale[i]) == pow10i(-_scale[i]):"scale out of bounds!,  col = " + i + ", scale = " + _scale[i];
        cols[i]._scale  = (short)pow10i(-_scale[i]);
        cols[i]._off    = (short)off;
        cols[i]._size   = (byte)colSizes[_colTypes[i]];
        cols[i]._domain = new ValueArray.ColumnDomain(_colDomains[i]);
        cols[i]._max    = _max[i];
        cols[i]._min    = _min[i];
        cols[i]._mean   = _mean[i];
        cols[i]._sigma  = _sigma[i];
        cols[i]._name   =  colNames == null ? String.valueOf(i) : colNames[i];
        off +=  Math.abs(cols[i]._size);
      }
      // finally make the value array header
      ValueArray ary = ValueArray.make(_resultKey, Value.ICE, dataset._key, "basic_parse", _numRows, off, cols);
      DKV.put(_resultKey, ary);
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
        }
        _invalidValues = new long[_ncolumns];
        switch(_phase){
        case 0:
          _min = new double [_ncolumns];
          Arrays.fill(_min, Double.MAX_VALUE);
          _max = new double[_ncolumns];
          Arrays.fill(_max, Double.MIN_VALUE);
          _mean = new double[_ncolumns];
          _scale = new int[_ncolumns];
          _colTypes = new byte[_ncolumns];
          FastParser p = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
          p.parse(key,skipFirstLine);
          if(arraylet) {
            assert (_nrows[ValueArray.getChunkIndex(key)] == 0) : ValueArray.getChunkIndex(key)+": "+Arrays.toString(_nrows)+" ("+_nrows[ValueArray.getChunkIndex(key)]+" -- "+_myrows+")";
            _nrows[ValueArray.getChunkIndex(key)] = _myrows;
          }
          break;
        case 1:
          _invalidValues = new long[_ncolumns];
          _sigma = new double[_ncolumns];
          _rowsize = 0;
          for(byte b:_colTypes) _rowsize += Math.abs(colSizes[b]);
          _lastOffset = -_rowsize;
          int rpc = (int)ValueArray.chunk_size()/_rowsize;
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
            _outputStreams[j] = new Stream(chunkRows*_rowsize);
            rowsToParse -= chunkRows;
            chunkRows = Math.min(rowsToParse,rpc);
            ++j;
          }
          _s = _outputStreams[0];
          FastParser p2 = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
          p2.parse(key,skipFirstLine);
          int inChunkOffset = (firstRow % rpc) * _rowsize; // index into the chunk I am writing to
          int lastChunk = Math.max(1,this._numRows / rpc) - 1; // index of the last chunk in the VA
          int chunkIndex = firstRow/rpc; // index of the chunk I am writing to
          if (chunkIndex > lastChunk) {
            assert (chunkIndex == lastChunk + 1);
            inChunkOffset += rpc * _rowsize;
            --chunkIndex;
          }
          for (int i = 0; i < _outputStreams.length; ++i) {
            Key k = ValueArray.make_chunkkey(_resultKey,ValueArray.chunk_offset(chunkIndex));
            assert (_outputStreams[i]._off == _outputStreams[i]._buf.length);
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
      try {
        DParseTask other = (DParseTask)drt;
        if(_sigma == null)_sigma = other._sigma;
        if(_invalidValues == null){
          _enums = other._enums;
          _min = other._min;
          _max = other._max;
          _mean = other._mean;
          _sigma = other._sigma;
          _scale = other._scale;
          _colTypes = other._colTypes;
          _nrows = other._nrows;
          _invalidValues = other._invalidValues;
        } else {
          if (_phase == 0) {
            if (_nrows != other._nrows)
              for (int i = 0; i < _nrows.length; ++i)
                _nrows[i] += other._nrows[i];
            for(int i = 0; i < _ncolumns; ++i) {
              if(_enums != other._enums){

              }
              if(other._min[i] < _min[i])_min[i] = other._min[i];
              if(other._max[i] > _max[i])_max[i] = other._max[i];
              if(other._scale[i] > _scale[i])_scale[i] = other._scale[i];
              if(other._colTypes[i] > _colTypes[i])_colTypes[i] = other._colTypes[i];
              _mean[i] += other._mean[i];
            }
          } else if(_phase == 1) {
            for(int i = 0; i < _ncolumns; ++i)
                _sigma[i] += other._sigma[i];
          } else
            assert false:"unexpected _phase value:" + _phase;
          for(int i = 0; i < _ncolumns; ++i)
            _invalidValues[i] += other._invalidValues[i];
        }
        _myrows += other._myrows;
        if(_error == null)_error = other._error;
        else if(other._error != null) _error = _error + "\n" + other._error;
      } catch (Exception e) {
        e.printStackTrace();
      }
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
      10000000000.0,
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
      if(exp < 0){
        System.out.println("haha");
      }
      assert 10 >= exp && exp >= 0:"unexpceted exponent " + exp;
      return powers10i[exp];
    }


    transient Stream _s;

    @SuppressWarnings("fallthrough")
    private void calculateColumnEncodings(){
      assert (_bases != null);
      assert (_min != null);
      for(int i = 0; i < _ncolumns; ++i){
        switch(_colTypes[i]){
        case ECOL: // enum
          if(_enums[i].isKilled()){
            _max[i] = 0;
            _min[i] = 0;
            _colTypes[i] = STRINGCOL;
          } else {
            _max[i] = _colDomains[i].length-1;
            _min[i] = 0;
            if(_max[i] < 256)_colTypes[i] = BYTE;
            else if(_max[i] < 65536)_colTypes[i] = SHORT;
            else _colTypes[i] = INT;
          }
          break;
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
        }
      }
    }

    public void newLine() {
      ++_myrows;
      if (_phase != 0) {
        _lastOffset += _rowsize;
        // to make sure that all rows are the same size, even if there are
        // missing columns
        if (_lastOffset > _s._off)
          _s._off = _lastOffset;
        if(_myrows > _outputRows[_outputIdx]) {
          ++_outputIdx;
          // this can happen if the last line ends in EOL. However this also
          // means that we will never write to the stream again, so it is ok.
          if (_outputIdx == _outputStreams.length) {
            _s = null;
          } else {
            _s = _outputStreams[_outputIdx];
            _myrows = 1;
            _lastOffset = 0;
          }
        }
      }
    }

    public void rollbackLine() {
      --_myrows;
      if(_phase != 0 && _s != null)
        System.out.println("haha");
      assert (_phase == 0 || _s == null);
    }

    public void addCol(int colIdx, double value) {
      if (Double.isNaN(value)) {
        addInvalidCol(colIdx);
      } else {
        double  d= value;
        int exp = 0;
        long number = (long)d;
        while (number != d) {
          d = d * 10;
          --exp;
          number = (long)d;
        }
        addNumCol(colIdx, number, exp, 1);
      }
    }
    
    
    public void setColumnNames(String[] colNames) {
      // NOT IMPLEMENTED YET
    }



    public void addInvalidCol(int colIdx){
      if(colIdx >= _ncolumns)
        return;
      ++_invalidValues[colIdx];
      if(_phase == PHASE_ONE)
        return;
      switch (_colTypes[colIdx]) {
        case BYTE:
          _s.set1(-1);
          break;
        case SHORT:
        case DSHORT:
          _s.set2(-1);
          break;
        case INT:
          _s.set4(Integer.MIN_VALUE);
          break;
        case LONG:
          _s.set8(Long.MIN_VALUE);
          break;
        case FLOAT:
          _s.set4f(Float.NaN);
          break;
        case DOUBLE:
          _s.set8d(Double.NaN);
          break;
        default:
          assert false:"illegal case: " + _colTypes[colIdx];
      }
    }

    public static final int MAX_ENUM_ELEMS = 65000;

    public void addStrCol(int colIdx, ValueString str){
      if(colIdx >= _ncolumns)
        return;
      switch (_phase) {
        case PHASE_ONE:
          Enum e = _enums[colIdx];
          if(e == null)return;
          if(_colTypes[colIdx] ==UCOL)
            _colTypes[colIdx] = ECOL;
          e.getTokenId(str);
          if(e.size() > MAX_ENUM_ELEMS)
            e.kill();
          ++_invalidValues[colIdx]; // invalid count in phase0 is in fact number of non-numbers (it is used fo mean computation, is recomputed in 2nd pass)
          break;
        case PHASE_TWO:
          if(_enums[colIdx] != null) {
            int id = _enums[colIdx].getTokenId(str);
            // we do not expect any misses here
            assert 0 <= id && id < _enums[colIdx].size();
            switch (_colTypes[colIdx]) {
            case BYTE:
              _s.set1(id);
              break;
            case SHORT:
              _s.set2(id);
              break;
            case INT:
              _s.set4(id);
              break;
            default:
              assert false:"illegal case: " + _colTypes[colIdx];
            }
          } else {
            addInvalidCol(colIdx);
          }
          break;
        default:
          assert (false);
      }
    }

    @SuppressWarnings("fallthrough")
    public void addNumCol(int colIdx, long number, int exp, int numLength) {
      if(colIdx >= _ncolumns)
        return;
      switch (_phase) {
        case PHASE_ONE:
          assert numLength >= 0:"invalid numLenght argument: " + numLength;
          double d = number*pow10(exp);
          if(d < _min[colIdx])_min[colIdx] = d;
          if(d > _max[colIdx])_max[colIdx] = d;
          _mean[colIdx] += d;
          if(exp < _scale[colIdx]) {
            _scale[colIdx] = exp;
            if(_colTypes[colIdx] != DCOL){
              if((float)d != d)
                _colTypes[colIdx] = DCOL;
              else
                _colTypes[colIdx] = FCOL;
            }
          } else if(_colTypes[colIdx] < ICOL) {
          _colTypes[colIdx] = ICOL;
          }
          break;
        case PHASE_TWO:
          switch(numLength) {
            case -1: // NaN
              addInvalidCol(colIdx);
              break;
            default:
              switch (_colTypes[colIdx]) {
                case BYTE:
                  _s.set1((byte)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
                  break;
                case SHORT:
                  _s.set2((short)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
                  break;
                case INT:
                  _s.set4((int)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
                  break;
                case LONG:
                  _s.set8(number*pow10i(exp - _scale[colIdx]));
                  break;
                case FLOAT:
                  _s.set4f((float)(number * pow10(exp)));
                  break;
                case DOUBLE:
                  _s.set8d(number * pow10(exp));
                  break;
                case DSHORT:
                  // scale is computed as negative in the first pass,
                  // therefore to compute the positive exponent after scale, we add scale and the original exponent
                  _s.set2((short)(number*pow10i(exp - _scale[colIdx]) - _bases[colIdx]));
                  break;
                case STRINGCOL:
                  break;
              }
          }
          // update sigma
          if(numLength > 0 && !Double.isNaN(_mean[colIdx])) {
            d = number*pow10(exp) - _mean[colIdx];
            _sigma[colIdx] += d*d;
          }
          break;
        default:
          assert (false);
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




