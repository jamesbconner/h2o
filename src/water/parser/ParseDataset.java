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

  static interface ParseHandler {
    public void handleRow(double[] nums, String[] strs);
    public void handleFinished(String[] headerRow);
  }
  static interface ParseEngine {
    void doParse(InputStream is, ParseHandler h) throws Exception;
  }

  // Configuration kind for parser
  private static final int PARSE_SVMLIGHT = 101;
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
      try {
        parseWithEngine(result, dataset, new XlsParser.Engine());
        return;
      } catch( Exception e ) { }

      Compression compression = guessCompressionMethod(dataset);
      if( compression == Compression.ZIP ) {
        // .xlsx files are actually zip files with xml inside them
        try {
          parseWithEngine(result, dataset, new XlsxParser.Engine());
          return;
        } catch( Exception e ) { }
      }

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
    int [] typeArr = guessParserSetup(dataset,false);
    switch( typeArr[PARSER_IDX] ) {
    case PARSE_SVMLIGHT:
      throw new Error("Parsing for SVMLIGHT is unimplemented");
    case PARSE_COMMASEP:
    case PARSE_SPACESEP:
      parseSeparated(result, dataset, (byte) typeArr[PARSER_IDX], typeArr[COLNUM_IDX]);
      break;
    default: H2O.unimpl();
    }

  }

//  public static void parseWithEngine(Key result, Value dataset, ParseEngine e) throws Exception {
//    final ParseState state = new ParseState();
//    state._num_rows  = 0; // No rows yet
//    state._num_cols  = 0; // No rows yet
//
//    e.doParse(dataset.openStream(), new ParseHandler() {
//      @Override
//      public void handleRow(double[] rowNums, String[] rowStrs) {
//        if( state._num_cols == 0 ) {
//          state._num_cols = rowNums.length;
//          state.prepareForStatsGathering();
//        }
//        if( allNaNs(rowNums) ) return;
//        state.addRowToStats(new Row(rowNums, rowStrs));
//      }
//
//      @Override
//      public void handleFinished(String[] names) {
//        state.finishStatsGathering(0);
//        state.maybeAssignColumnNames(names);
//      }
//    });
//
//    state.filterColumnsDomains();
//    state.computeColumnSize();
//    final int row_size = state.computeOffsets();
//    final byte[] buf = MemoryManager.allocateMemory(state._num_rows*row_size);
//    final double[] sumerr = new double[state._num_cols];
//    final HashMap<String,Integer> columnIndexes[] = state.createColumnIndexes();
//    e.doParse(dataset.openStream(), new ParseHandler() {
//      int off = 0;
//      @Override
//      public void handleRow(double[] rowNums, String[] rowStrs) {
//        if( allNaNs(rowNums) ) return;
//        state.addRowToBuffer(buf, off, sumerr, new Row(rowNums, rowStrs), columnIndexes);
//        off += row_size;
//      }
//
//      @Override public void handleFinished(String[] headerRow) { }
//    });
//
//    state.normalizeVariance(sumerr);
//    packRowsIntoValueArrayChunks(result, 0, state._num_rows, row_size, state, buf, null);
//    // Now make the structured ValueArray & insert the main key
//    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "excel_parse",
//        state._num_rows, row_size, state._cols);
//    UKV.put(result,ary);
//  }

  public static void parseSeparated(Key result, Value dataset, byte parseType, int num_cols) {

    DParseStatsPass dp1 = new DParseStatsPass();
    dp1._parseType = parseType;
    ParseState state = dp1._state;
    state._num_cols  = num_cols;
    state._num_rows  = 0; // No rows yet
    dp1.invoke(dataset._key); // Parse whole dataset!
    assert state == dp1._state;

    // #1: Guess Column names, if any
    String[] names = null;
    try { // to be robust here have a fail-safe mode but log the exception
          // NOTE: we should log such fail-safe situations
      names = guessColNames(dataset, num_cols, parseType);
    } catch( Exception e ) {
      System.err.println("[parser] Column names guesser failed.");
      e.printStackTrace(System.err);
    }
    state.maybeAssignColumnNames(names);
    state.filterColumnsDomains();
    state.computeColumnSize();
    int row_size = state.computeOffsets();

    // Setup for pass-2, where we do the actual data conversion. Also computes variance.
    DParseCollectDataPass dp2 = new DParseCollectDataPass();
    dp2._parseType = parseType;
    dp2._row_size = row_size;
    dp2._state  = state.clone();
    dp2._result = result;
    dp2.invoke(dataset._key);   // Parse whole dataset!

    state.normalizeVariance(dp2._sumerr);
    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "basic_parse", state._num_rows, row_size, state._cols);
    UKV.put(result,ary);

    // Done building the result ValueArray!
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

  // Helper class containing column domain and providing its serialization and deserialization.
  // Note: helper class expect the maximum size of the domain as stated in DOMAIN_MAX_BYTE_SIZE - i.e., 2 bytes
  static public class ColumnDomain {
    // Maximum size (in bytes) of column which contains enum (= limited number of string values)
    public static final byte DOMAIN_MAX_BYTE_SIZE = 2;
    public static final int  DOMAIN_MAX_VALUES    = (1 << DOMAIN_MAX_BYTE_SIZE*8)-1;

    // Include all domain values in their insert-order.
    LinkedHashSet<String> _domainValues;
    //
    boolean _killed;

    public ColumnDomain() {
      // Domain values are stored in the set which preserve insert order.
      _domainValues = new LinkedHashSet<String>();
      _killed       = false;
    }

    public final int     size()     { return _domainValues.size(); }
    public final boolean isKilled() { return _killed; }
    public final void    kill()     {
      if (!_killed) {
        _killed = true;
        _domainValues.clear();
      }
    }

    public final void add(String s) {
      if( s == null ) return;   // Silently ignore nulls
      if (_killed) return; // this column domain is not live anymore (too many unique values)
      if (_domainValues.size() == DOMAIN_MAX_VALUES) kill();
      else _domainValues.add(s);
    }

    public void write( DataOutputStream dos ) throws IOException {
      // write size of domain
      dos.writeShort(_killed ? 65535 : _domainValues.size());
      // write domain values
      for (String s : _domainValues) {
        dos.writeShort(s.length()); // Note: we do not expect to have domain names longer than > 2^16 characters
        dos.write(s.getBytes());
      }
    }

    public void write( Stream s ) {
      final int off = s._off;
      s.set2(_killed ? 65535 : _domainValues.size());
      for( String sv : _domainValues)
        s.setLen2Str(sv);
      assert off+wire_len() == s._off;
    }

    static public ColumnDomain read( DataInputStream dis ) throws IOException {
      ColumnDomain cd = new ColumnDomain();
      int domainSize  = dis.readChar();
      cd._killed = (domainSize==65535);
      if( !cd._killed ) {
        for (int i = 0; i < domainSize; i++) {
          int len     = dis.readShort();
          byte name[] = new byte[len];
          dis.readFully(name);
          cd._domainValues.add(new String(name));
        }
      }
      return cd;
    }

    static public ColumnDomain read( Stream s ) {
      final int off = s._off;
      ColumnDomain cd = new ColumnDomain();
      int domainSize  = s.get2();
      cd._killed = (domainSize==65535);
      if( !cd._killed ) {
        for( int i = 0; i < domainSize; i++)
          cd._domainValues.add(s.getLen2Str());
      }
      assert off+cd.wire_len() == s._off;
      return cd;
    }

    public final int wire_len() {
      int res = 2;              // 2bytes to store size of domain: 2 bytes
      for (String s : _domainValues)
        res += 2+s.length() ;   // 2bytes to store string length + string bytes
      return res;
    }

    // Union of two column enum domains. If the union is
    public final ColumnDomain union(ColumnDomain columnDomain) {
      if (_killed) return this; // killed domains cannot be extended any more
      if (columnDomain._killed) {
        kill();
        return this;
      }

      _domainValues.addAll(columnDomain._domainValues);

      // check the size after union - if the domain is to big => kill it
      if (_domainValues.size() > DOMAIN_MAX_VALUES) kill();

      return this;
    }

    // For testing
    public final String[] toArray() { return  _domainValues.toArray(new String[_domainValues.size()]); }
  }

  public static final class DParseTask extends MRTask {
    static final byte MCOL = -2;  // mixed numbers and strings
    static final byte SCOL = -3;  // string column (too many enum values)
    // pass 1 types
    static final byte UCOL =  0;  // unknown
    static final byte ICOL =  1;  // number column
    static final byte FCOL =  2;  // number column
    static final byte DCOL =  3;  // number column
    static final byte ECOL =  4;  // number column
    // pass 2 types
    static final byte BYTE_ENUM  = 5;
    static final byte SHORT_ENUM = 6;
    static final byte INT_ENUM   = 7;
    static final byte BYTE       = 8;
    static final byte BYTE_BASE  = 9;
    static final byte SHORT      = 10;
    static final byte SHORT_BASE = 11;
    static final byte INT        = 12;
    static final byte INT_BASE   = 13;
    static final byte LONG       = 14;
    static final byte FLOAT      = 15;
    static final byte DOUBLE     = 16;
    static final byte BSHORT     = 17;
    static final byte DSHORT     = 18;
    static final byte DSHORT_BASE= 19;

    int _phase;
    int _ncolumns;
    byte _sep = (byte)',';
    byte _decSep = (byte)'.';
    String _error;
    byte     [] _colTypes;
    int      [] _scale;
    long     [] _invalidValues;
    double   [] _min;
    double   [] _max;
    boolean  [] _floats;
    FastTrie [] _enums;

    @Override public int wire_len() {
      switch(_phase){
      case 0:
        return 4 + 4 + 1 + 1;
      }
      return 0;
      }

    @Override public void write( Stream s )                                {
      s.set4(_phase);
      s.set4(_ncolumns);
      s.set1(_sep);
      s.set1(_decSep);
      switch(_phase){
      case 0:
        break;
      case 1:
        s.setAry1(_colTypes);
        s.setAry4(_scale);
        s.setAry8(_invalidValues);
        s.setAry8d(_min);
        s.setAry8d(_max);
        int n = 0;
        for(int i = 0; i < _ncolumns;++i)
          if(_colTypes[i] == ECOL || _colTypes[i] == MCOL)++n;
        s.set4(n);
        for(int i = 0; i < _ncolumns; ++i){

        }
      default:
        throw new Error("illegal phase " + _phase);
      }

    }
    @Override public void read ( Stream s ) {
      _phase = s.get4();
      _ncolumns = s.get4();
      _sep = s.get1();
      _decSep = s.get1();
      switch(_phase){
      case 0:
        break;
      case 1:
        _colTypes = s.getAry1();
        _scale = s.getAry4();
        _invalidValues = s.getAry8();
        _min = s.getAry8d();
        _max = s.getAry8d();
        int n = s.get4();
        if(n > 0){
          _enums = new FastTrie[n];
          for(int i = 0; i < n; ++i){
            _enums[i] = new FastTrie();
            _enums[i].read(s);
          }
        }
        break;
      case 2:
        break;
      case 3:
        break;
      default:
        throw new Error("illegal phase " + _phase);
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
      _colTypes = new byte[_ncolumns];
      try{
        FastParser p = new FastParser(aryKey, _ncolumns, _sep, _decSep, this);
        p.parse(key,true);
        ++_phase;
      }catch(Exception e){
        _error = e.getMessage();
      }
    }


    @Override
    public void reduce(DRemoteTask drt) {
      throw new RuntimeException("TODO Auto-generated method stub");
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

    static double pow10(int exp){
      return ((exp >= -10 && exp <= 10)?powers10[exp+10]:Math.pow(10, exp));
    }

    double [] _dbases;
    long   [] _lbases;

    private void calculateColumnEncodings(){
      for(int i = 0; i < _ncolumns; ++i){
        switch(_colTypes[i]){
        case ICOL: // number
          if (_max[i] - _min[i] <= 256) {
            if (_max[i] <= Byte.MAX_VALUE && _min[i] >= Byte.MIN_VALUE)
              _colTypes[i] = BYTE;
            else {
              _colTypes[i] = BYTE_BASE;
              _lbases[i] = Byte.MIN_VALUE - (long)_min[i];
            }
          } else if ((_max[i] - _min[i]) <= 65536) {
            if (_max[i] <= Short.MAX_VALUE && _min[i] >= Short.MIN_VALUE)
              _colTypes[i] = SHORT;
            else {
              _colTypes[i] = SHORT_BASE;
              _lbases[i] = Short.MIN_VALUE - (long)_min[i];
            }
          } else if (_max[i] - _min[i] <= (1l << 32)) {
            if (_max[i] <= Integer.MAX_VALUE && _min[i] >= Integer.MIN_VALUE)
              _colTypes[i] = INT;
            else {
              _colTypes[i] = INT_BASE;
              _lbases[i] = Integer.MIN_VALUE - (long)_min[i];
            }
          } else {
            _colTypes[i] = LONG;
          }
          break;
        case FCOL:
        case DCOL:
          double s = pow10(_scale[i]);
          double range = s*(_max[i]-_min[i]);
          if(range <= 65536){
            double max = s*_max[i];
            double min = s*_min[i];
            if(max <= Short.MAX_VALUE && min >= Short.MIN_VALUE)
              _colTypes[i] = DSHORT;
            else {
              _colTypes[i] = DSHORT_BASE;
              _dbases[i] = Short.MIN_VALUE - min;
            }
          } // else leave it as float/double
        case ECOL: // enum
          if(_enums[i]._nfinalStates <= 256)_colTypes[i] = BYTE_ENUM;
          else if(_enums[i]._nfinalStates <= 65536)_colTypes[i] = SHORT_ENUM;
          else _colTypes[i] = INT_ENUM;
        }
      }
    }

    public void addRow(FastParser.Row row){
      switch (_phase) {
      case 0:
        for(int i = 0; i < _ncolumns; ++i){
          if(row._exponents[i] == Integer.MAX_VALUE){
            ++_invalidValues[i];
            if(row._numbers[i] < 0){
              continue; //NaN
            }
            // enum
            switch(_colTypes[i]){
            case UCOL:
              _colTypes[i] = ECOL;
            case ECOL:
              if(row._numbers[i] == -1)_colTypes[i] = SCOL;
              break;
            case NCOL:
              _colTypes[i] = MCOL;
            case MCOL:
              ++_invalidValues[i];
            default:
              break;
            }
          } else { // number
              switch(_colTypes[i]){
              case ECOL:
                _colTypes[i] = MCOL;
                break;
              case UCOL:
                _colTypes[i] = NCOL;
                break;
              }
              int exp = row._numLength[i] + row._exponents[i];
              if(exp < 0 && exp < _scale[i])_scale[i] = exp;
              double d = row._numbers[i]*pow10(exp);
              if(d < _min[i])_min[i] = d;
              if(d > _max[i])_max[i] = d;
              if(_floats[i] && (float)d != d)_floats[i] = false;
            }
          }
      case 2:
        for (int i = 0; i < row._numbers.length; ++i) {
          long number = row._numbers[i];
          int exp = row._exponents[i];
          switch (_colTypes[i]) {
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
    }



  // ----
  // Distributed parsing.

  // Just the common fields being moved over the wire during parse compaction.
  public static abstract class DParse extends MRTask {
    ParseState _state;
    public DParse() { _state = new ParseState(); }

    @Override
    protected DParse clone() throws CloneNotSupportedException {
      DParse dp = (DParse) super.clone();
      dp._state = _state.clone();
      return dp;
    }

    // Hand-rolled serializer for the above common fields.
    // Some Day Real Soon auto-gen me.
    @Override public int wire_len() { return _state.wire_len(); }
    @Override public void write( Stream s )                                { _state.write(s);   }
    @Override public void read ( Stream s )                                { _state.read(s);    }
    @Override public void write( DataOutputStream dos ) throws IOException { _state.write(dos); }
    @Override public void read ( DataInputStream dis  ) throws IOException { _state.read(dis);  }
  }

  // ----
  // Distributed parsing, Pass 1
  // Find min/max, digits per column.  Find number of rows per chunk.
  // Collects columns' domains (in case the column contains string values).
  public static class DParseStatsPass extends DParse {
    byte _parseType;

    public void map( Key key ) {
      _state.prepareForStatsGathering();
      SeparatedValueParser csv = new SeparatedValueParser(key,
          _parseType == PARSE_COMMASEP ? ',' : ' ',
          _state._cols.length);
      for( Row row : csv ) {
        if( !allNaNs(row._fieldVals) ) _state.addRowToStats(row);
      }
      _state.finishStatsGathering(key.user_allowed() ? 0 : ValueArray.getChunkIndex(key));
    }

    // Combine results
    public void reduce( DRemoteTask rt ) {
      _state.mergeStats(((DParseStatsPass)rt)._state);
    }
  }

  // ----
  // Distributed parsing, Pass 2
  // Parse the data, and jam it into compressed fixed-sized row data
  public static class DParseCollectDataPass extends DParse {
    Key _result;
    double[] _sumerr;
    int _row_size;
    byte _parseType;

    // Parse just this chunk, compress into new format.
    public void map( Key key ) {
      assert _state._cols != null;

      // Get chunk index
      int cidx = key.user_allowed() ? 0 : ValueArray.getChunkIndex(key);
      int start_row = _state._rows_chk[cidx];
      int num_rows = _state._rows_chk[cidx+1]-start_row;

      // Get a place to hold the data
      byte[] buf = MemoryManager.allocateMemory(num_rows*_row_size);
      // A place to hold each column datum
      _sumerr = new double[_state._num_cols];
      // The parser
      SeparatedValueParser csv = new SeparatedValueParser(key,
          _parseType == PARSE_COMMASEP ? ',' : ' ', _state._cols.length);

      // Prepare hashmap for each column domain to get domain item's index quickly
      HashMap<String,Integer> columnIndexes[] = _state.createColumnIndexes();

      // Fill the rows
      int off = 0;
      for( Row row : csv ) {
        if( allNaNs(row._fieldVals) ) continue; // Row is dead, skip it entirely
        _state.addRowToBuffer(buf, off, _sumerr, row, columnIndexes);
        off += _row_size;
      }

      packRowsIntoValueArrayChunks(_result, start_row, num_rows, _row_size, _state, buf, this);

      // Reset these arrays to null, so they are not part of the return result.
      _state._rows_chk = null;
      _state._cols = null;
      _state._cols_domains = null;
      _state._num_rows = 0;            // No data to return
    }

    public void reduce( DRemoteTask rt ) {
      // return the variance
      DParseCollectDataPass dp2 = (DParseCollectDataPass)rt;
      if( _sumerr == null )
        _sumerr = dp2._sumerr;
      else {
        for(int i = 0; i < _sumerr.length; ++i)
          _sumerr[i] += dp2._sumerr[i];
      }
      _state._cols = null;
      _state._num_rows = 0;
    }
  }

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

  // ---
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
  public static String[] guessColNames( Value dataset, int num_cols, byte csvType ) {
    SeparatedValueParser csv = new SeparatedValueParser(dataset.chunk_get(0),
        csvType == PARSE_COMMASEP ? ',' : ' ', num_cols);
    Iterator<Row> it = csv.iterator();
    if( !it.hasNext() ) return null;
    return it.next()._fieldStringVals;
  }
}
