
package water.parser;
import init.H2OSerializable;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.*;

import water.*;
import water.ValueArray.Column;
import water.nbhm.NonBlockingHashMap;

import com.google.common.io.Closeables;
import java.util.ArrayList;

/**
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

// Guess
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
  public static void parseUncompressed( Key result, Value dataset ) throws Exception {
    String datasetName = new String(dataset._key._kb);
//    if(datasetName.endsWith(".xls") || datasetName.contains(".xls."))
//      throw new Error("xls format is currently not supported.");
    DParseTask phaseOne = DParseTask.createPhaseOne(dataset, result, CustomParser.Type.XLSX);
    phaseOne.phaseOne();
    DParseTask phaseTwo = DParseTask.createPhaseTwo(phaseOne);
    phaseTwo.phaseTwo();
    
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
    // create and used only on the task caller's side
    transient String[][] _colDomains;


    
    /** Manages the chunk parts of the result hex varray. 
     * 
     * Second pass parse encodes the data from the source file to the sequence
     * of these stream objects. Each stream object will always go to a single
     * chunk (but a single chunk can contain more stream objects). To manage
     * this situation, the list of stream records is created upfront and then
     * filled automatically. 
     * 
     * Stream record then knows its chunkIndex, that is which chunk it will be
     * written to, the offset into that chunk it will be written and the number
     * of input rows that will be parsed to it.
     */
    final class OutputStreamRecord {
      final int _chunkIndex;
      final int _chunkOffset;
      final int _numRows;
      Stream _stream;

      /** Allocates the stream for the chunk. Streams should only be allocated
       * right before the record will be used and should be stored immediately
       * after that. 
       */
      public Stream initialize() {
        assert (_stream == null);
        _stream = new Stream(_numRows * _rowsize);
        return _stream;
      }
      
      /** Stores the stream to its chunk using the atomic union. After the data
       * from the stream is stored, its memory is freed up. 
       */
      public void store() {
        assert (_stream != null);
        if (!_stream.eof())
          assert (_stream.eof());
        Key k = ValueArray.make_chunkkey(_resultKey,ValueArray.chunk_offset(_chunkIndex));
        AtomicUnion u = new AtomicUnion(_stream._buf,0,_chunkOffset,_stream._buf.length);
        lazy_complete(u.fork(k));
        _stream = null; // free mem
      }
      
      // You are not expected to create record streams yourself, use the 
      // createRecords method of the DParseTask.
      protected OutputStreamRecord(int chunkIndex, int chunkOffset, int numRows) {
        _chunkIndex = chunkIndex;
        _chunkOffset = chunkOffset;
        _numRows = numRows;
      }
    }
    
    /** Returns the list of streams that should be used to store the given rows.
     * 
     * None of the returned streams is initialized. 
     * 
     * @param firstRow
     * @param rowsToParse
     * @return 
     */
    protected OutputStreamRecord[] createRecords(long firstRow, int rowsToParse) {
      assert (_rowsize != 0);
      ArrayList<OutputStreamRecord> result = new ArrayList();
      int rpc = (int) ValueArray.chunk_size() / _rowsize;
      int rowInChunk = (int)firstRow % rpc;
      int lastChunk = Math.max(1,this._numRows / rpc) - 1; // index of the last chunk in the VA
      int chunkIndex = (int)firstRow/rpc; // index of the chunk I am writing to
      if (chunkIndex > lastChunk) { // we can be writing to the second chunk after its real boundary
        assert (chunkIndex == lastChunk + 1);
        rowInChunk += rpc;
        --chunkIndex;
      }
      do {
        // number of rows that go the the current chunk - all remaining rows for the
        // last chunk, or the number of rows that can go to the chunk
        int rowsToChunk = (chunkIndex == lastChunk) ? rowsToParse : Math.min(rowsToParse, rpc - rowInChunk); 
        // add the output stream reacord
        result.add(new OutputStreamRecord(chunkIndex, rowInChunk * _rowsize, rowsToChunk));
        // update the running variables
        if (chunkIndex < lastChunk) {
          rowInChunk = 0;
          ++chunkIndex;
        }
        rowsToParse -= rowsToChunk;
        assert (rowsToParse >= 0);
      } while (rowsToParse > 0);
      
      _outputIdx = 0;
      _lastOffset = - _rowsize;
      // return all output streams 
      return result.toArray(new OutputStreamRecord[result.size()]);
    } 

    // 
    transient OutputStreamRecord[] _outputStreams2;
    transient Stream _s;
    transient int _lastOffset;
    transient int    _outputIdx;
    
    transient String[] _colNames;

    
    
    transient final CustomParser.Type _parserType;
    transient final Value _sourceDataset;
    
    
    // As this is only used for distributed CSV parser we initialize the values
    // for the CSV parser itself.
    public DParseTask() {
      _parserType = CustomParser.Type.CSV;
      _sourceDataset = null;
    }
    
    private DParseTask(Value dataset, Key resultKey, CustomParser.Type parserType) {
      _parserType = parserType;
      _sourceDataset = dataset;
      _resultKey = resultKey;
      _phase = PHASE_ONE;
    }

    private DParseTask(DParseTask other) {
      assert (other._phase == PHASE_ONE);
      // copy the phase one data
      // don't pass invalid values, we do not need them 2nd pass
      _parserType = other._parserType;
      _sourceDataset = other._sourceDataset;
      _enums = other._enums;
      _colTypes = other._colTypes;
      _nrows = other._nrows;
      _skipFirstLine = other._skipFirstLine;
      _myrows = other._myrows; // for simple values, number of rows is kept in the member variable instead of _nrows
      _resultKey = other._resultKey;
      _colTypes = other._colTypes;
      _nrows = other._nrows;
      _numRows = other._numRows;
      _sep = other._sep;
      _decSep = other._decSep;
      _scale = other._scale;
      _ncolumns = other._ncolumns;
      _min = other._min;
      _max = other._max;
      _mean = other._mean;
      _sigma = other._sigma;
      _colNames = other._colNames;
    }
    
    
    
    public static DParseTask createPhaseOne(Value dataset, Key resultKey, CustomParser.Type parserType) {
      return new DParseTask(dataset,resultKey,parserType); 
    }
    
    public void phaseOne() throws Exception { 
      switch (_parserType) {
        case CSV:
          // precompute the parser setup, column setup and other settings
          byte [] bits = DKV.get(_sourceDataset.chunk_get(0)).get(256*1024);
          int [] psetup = FastParser.guessParserSetup(bits, false);
          _colNames = FastParser.determineColumnNames(bits,(byte)psetup[0]);
          // initialize the column names
          // TODO Parser setup is aparently not working properly
          if (_colNames!=null) {
            // use the column names found in the header if any
            psetup[1] = _colNames.length;
            setColumnNames(_colNames);
          } else {
            // otherwise initialize the column names appropriately
            _colNames = new String[psetup[1]];
            for (int i = 0; i < psetup[1]; ++i)
              _colNames[i] = String.valueOf(i);
            setColumnNames(_colNames);
          }
          _skipFirstLine = _colNames != null;
          // set the separator
          this._sep = (byte) psetup[0];
          // if parsing value array, initialize the nrows array
          if (_sourceDataset instanceof ValueArray) {
            ValueArray ary = (ValueArray) _sourceDataset;
            _nrows = new int[(int)ary.chunks()];
          }
          // launch the distributed parser on its chunks. 
          this.invoke(_sourceDataset._key);
          break;
        case XLS:
        case XLSX:
          // XLS parsing is not distributed, just obtain the value stream and
          // run the parser
          CustomParser p = (_parserType == CustomParser.Type.XLS) ? new XlsParser(this) : new XlsxParser(this);
          p.parse(_sourceDataset._key); 
          --_myrows; // do not count the header
          break;
        default:
          throw new Error("NOT IMPLEMENTED");
      }
      // calculate proper numbers of rows for the chunks
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
    }
    
    public static DParseTask createPhaseTwo(DParseTask phaseOneTask) {
      DParseTask t = new DParseTask(phaseOneTask);
      // create new data for phase two
      t._colDomains = new String[t._ncolumns][];
      t._bases = new int[t._ncolumns];
      t._phase = PHASE_TWO;
      // calculate the column domains
      for(int i = 0; i < t._colTypes.length; ++i){
        if(t._colTypes[i] == ECOL && t._enums[i] != null && !t._enums[i].isKilled())
          t._colDomains[i] = t._enums[i].computeColumnDomain();
        else
          t._enums[i] = null;
      }
      t.calculateColumnEncodings();
      return t;
    }
    
    public void phaseTwo() throws Exception {
      switch (_parserType) {
        case CSV:
          // for CSV parser just launch the distributed parser on the chunks
          // again
          this.invoke(_sourceDataset._key);
          break;
        case XLS:
        case XLSX:
          // initialize statistics - invalid rows, sigma and row size
          phaseTwoInitialize();
          // create the output streams
          _outputStreams2 = createRecords(0, _myrows);
          assert (_outputStreams2.length > 0);
          _s = _outputStreams2[0].initialize();
          // perform the second parse pass
          CustomParser p = (_parserType == CustomParser.Type.XLS) ? new XlsParser(this) : new XlsxParser(this);
          p.parse(_sourceDataset._key);
          // store the last stream if not stored during the parse
          if (_s != null) 
            _outputStreams2[_outputIdx].store();
          break;
        default:
          throw new Error("NOT IMPLEMENTED");
      }
      // normalize sigma
      for(int i = 0; i < _ncolumns; ++i)
        _sigma[i] = Math.sqrt(_sigma[i]/(_numRows - _invalidValues[i]));
      createValueArrayHeader();
    }


    public void createValueArrayHeader() {
      assert (_phase == PHASE_TWO);
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
        cols[i]._name   = _colNames[i];
        off +=  Math.abs(cols[i]._size);
      }
      // finally make the value array header
      ValueArray ary = ValueArray.make(_resultKey, Value.ICE, _sourceDataset._key, "basic_parse", _numRows, off, cols);
      DKV.put(_resultKey, ary);
    }

    /** Initialize phase one data structures with the appropriate number of
     * columns.
     *
     * @param numColumns
     */
    public void phaseOneInitialize() {
      if (_phase != PHASE_ONE)
        assert (_phase == PHASE_ONE);
      _invalidValues = new long[_ncolumns];
      _min = new double [_ncolumns];
      Arrays.fill(_min, Double.MAX_VALUE);
      _max = new double[_ncolumns];
      Arrays.fill(_max, Double.MIN_VALUE);
      _mean = new double[_ncolumns];
      _scale = new int[_ncolumns];
      _colTypes = new byte[_ncolumns];
    }

    /** Initializes the output streams.
     */
    public void phaseTwoInitialize() {
      assert (_phase == PHASE_TWO);
      _invalidValues = new long[_ncolumns];
      _sigma = new double[_ncolumns];
      _rowsize = 0;
      for(byte b:_colTypes) _rowsize += Math.abs(colSizes[b]);
    }
    
    /** Map function for distributed parsing of the CSV files.
     * 
     * In first phase it calculates the min, max, means, encodings and other
     * statistics about the dataset, determines the number of columns.
     * 
     * The second pass then encodes the parsed dataset to the result key,
     * splitting it into equal sized chunks. 
     * 
     * @param key 
     */
    @Override public void map(Key key) {
      try {
        Key aryKey = null;
        boolean arraylet = key._kb[0] == Key.ARRAYLET_CHUNK;
        boolean skipFirstLine = _skipFirstLine;
        if(arraylet) {
          aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
          _chunkId = ValueArray.getChunkIndex(key);
          skipFirstLine = skipFirstLine || (ValueArray.getChunkIndex(key) != 0);
        }
        switch (_phase) {
          case PHASE_ONE:
            assert (_ncolumns != 0);
            // initialize the column statistics 
            phaseOneInitialize();
            // perform the parse
            FastParser p = new FastParser(aryKey, _ncolumns, _sep, _decSep, this,skipFirstLine);
            p.parse(key);
            if(arraylet) {
              assert (_nrows[ValueArray.getChunkIndex(key)] == 0) : ValueArray.getChunkIndex(key)+": "+Arrays.toString(_nrows)+" ("+_nrows[ValueArray.getChunkIndex(key)]+" -- "+_myrows+")";
              _nrows[ValueArray.getChunkIndex(key)] = _myrows;
            }
            break;
          case PHASE_TWO:
            assert (_ncolumns != 0);
            // initialize statistics - invalid rows, sigma and row size
            phaseTwoInitialize();
            // calculate the first row and the number of rows to parse
            int firstRow = 0;
            int lastRow = _myrows;
            _myrows = 0;
            if(arraylet){
              long origChunkIdx = ValueArray.getChunkIndex(key);
              firstRow = (origChunkIdx == 0) ? 0 : _nrows[(int)origChunkIdx-1];
              lastRow = _nrows[(int)origChunkIdx];
            }
            int rowsToParse = lastRow - firstRow;
            // create the output streams
            _outputStreams2 = createRecords(firstRow, rowsToParse);
            assert (_outputStreams2.length > 0);
            _s = _outputStreams2[0].initialize();
            // perform the second parse pass
            FastParser p2 = new FastParser(aryKey, _ncolumns, _sep, _decSep, this,skipFirstLine);
            p2.parse(key);
            // store the last stream if not stored during the parse
            if (_s != null) 
              _outputStreams2[_outputIdx].store();
            break;
          default:
            assert (false);
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
              if(_enums[i] != other._enums[i])
                _enums[i].merge(other._enums[i]);
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
      assert 10 >= exp && exp >= 0:"unexpceted exponent " + exp;
      return powers10i[exp];
    }



    @SuppressWarnings("fallthrough")
    private void calculateColumnEncodings(){
      assert (_bases != null);
      assert (_min != null);
      for(int i = 0; i < _ncolumns; ++i){
        switch(_colTypes[i]){
        case ECOL: // enum
          if(_enums[i] == null || _enums[i].isKilled()){
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

    
    /** Advances to new line. In phase two it also must make sure that the
     * 
     */ 
    public void newLine() {
      ++_myrows;
      if (_phase == PHASE_TWO) {
        _lastOffset += _rowsize;
        // check if the row was a full size row or not
        if (_lastOffset > _s._off) {
          // JIRA - maybe we might want to do domething else here - now it fills
          // the contents to the default value which is 0
          _s._off = _lastOffset;
        }
        // if we are at the end of current stream, move to the next one
        if (_s.eof()) {
          _outputStreams2[_outputIdx].store();
          ++_outputIdx;
          if (_outputIdx < _outputStreams2.length) {
            _s = _outputStreams2[_outputIdx].initialize();
            _lastOffset = 0;
          } else {
            _s = null; // just to be sure we throw a NPE if there is a problem
          }
        }
      }
    }

    public void rollbackLine() {
      --_myrows;
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


    /** Sets the column names and creates the array of the enums for each
     * column. 
     * 
     * @param colNames 
     */
    public void setColumnNames(String[] colNames) {
      if (_phase == PHASE_ONE) {
        assert (colNames != null);
        _colNames = colNames;
        _ncolumns = colNames.length;
        _enums = new Enum[_ncolumns];
        for(int i = 0; i < _ncolumns; ++i)
          _enums[i] = new Enum();
        // Initialize the statistics for the XLS parsers. Statistics for CSV
        // parsers are created in the map method - they must be different for
        // each distributed invocation
        if (_parserType == CustomParser.Type.XLS)
          phaseOneInitialize();
      }
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
        case STRINGCOL:
          // TODO, replace with empty space!
          _s.set1(-1);
          break;
        default:
          assert false:"illegal case: " + _colTypes[colIdx];
      }
    }


    public void addStrCol(int colIdx, ValueString str){
      if(colIdx >= _ncolumns)
        return;
      switch (_phase) {
        case PHASE_ONE:
          Enum e = _enums[colIdx];
          if(e == null || e.isKilled())return;
          if(_colTypes[colIdx] ==UCOL)
            _colTypes[colIdx] = ECOL;
          e.addKey(str);
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

  public static void main(String [] args){
    byte [] buff = "hahagaga".getBytes();
    ValueString str = new ValueString(buff,0,8);
    System.out.println(str.hashCode() + " " + "hahagaga".hashCode());
  }
}




