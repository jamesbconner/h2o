package water.parser;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.*;

import com.google.common.io.Closeables;

import water.*;
import water.parser.SeparatedValueParser.Row;

/**
 * Helper class to parse an entire ValueArray data, and produce a structured
 * ValueArray result.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public final class ParseDataset {
  static enum Compression { NONE, ZIP, GZIP }

  private static final byte[] XLSX_MAGIC = new byte[] { 0x50, 0x4B, 0x03, 0x04, 0x14, 0x00, 0x06, 0x00 };
  private static final byte[] XLS_MAGIC  = new byte[] { 0x09, 0x08, 0x10, 0x00, 0x00, 0x06, 0x05, 0x00 };
  private static final int XLS_MAGIC_OFFSET  = 512;
  private static final int XLSX_MAGIC_OFFSET = 0;

  // Configuration kind for parser
  private static final int PARSE_SVMLIGHT = 101;
  private static final int PARSE_COMMASEP = 102;
  private static final int PARSE_SPACESEP = 103;
  private static final int PARSE_EXCEL    = 104;

  // Index to array returned by method guesss_parser_setup()
  static final int PARSER_IDX = 0;
  static final int COLNUM_IDX = 1;

  // Parse the dataset (uncompressed, zippped) as a CSV-style thingy and produce a structured dataset as a
  // result.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");

    Compression compression = guess_compression_method(dataset);
    switch (compression) {
    case NONE: parseUncompressed(result, dataset); break;
    case ZIP : parseZipped(result, dataset);       break;
    case GZIP: parseGZipped(result, dataset);      break;
    default  : throw new Error("Uknown compression of dataset!");
    }
  }

 // Parse the uncompressed dataset as a CSV-style structure and produce a structured dataset
 // result.  This does a distributed parallel parse.
  public static void parseUncompressed( Key result, Value dataset ) {
    // Guess on the number of columns, build a column array.
    int [] typeArr = guessParserSetup(dataset,false);
    switch( typeArr[PARSER_IDX] ) {
    case PARSE_SVMLIGHT:
      throw new Error("Parsing for SVMLIGHT is unimplemented");
    case PARSE_EXCEL:
      parseExcel(result, dataset);
      break;
    case PARSE_COMMASEP:
    case PARSE_SPACESEP:
      parseSeparated(result, dataset, (byte) typeArr[PARSER_IDX], typeArr[PARSER_IDX]);
      break;
    default: H2O.unimpl();
    }

  }

  public static void parseExcel(Key result, Value dataset) {

  }

  public static void parseSeparated(Key result, Value dataset, byte parseType, int num_cols) {
    DParse1 dp1 = new DParse1();
    dp1._num_cols  = num_cols;
    dp1._parseType = parseType;
    dp1._num_rows  = 0; // No rows yet

    dp1.invoke(dataset._key);   // Parse whole dataset!

    num_cols = dp1._num_cols;

    // #1: Guess Column names, if any
    String[] names = null;
    try { // to be robust here have a fail-safe mode but log the exception
          // NOTE: we should log such fail-safe situations
      names = guess_col_names(dataset, num_cols, parseType);
    } catch (Exception e) {
      System.err.println("[parser] Column names guesser failed.");
    }
    for( int i=0; i<num_cols; i++ )
        dp1._cols[i]._name = names != null  ? names[i] : ""; // or String.valueOf(i)

    // #2: Filter columns which are too big (contains too many unique strings)
    // and manage domain overlap with column name.
    filter_columns_domains(dp1);
    // #3: Now figure out how best to represent the data.
    compute_column_size(dp1);

    // #4: Compute row size & column offsets
    int row_size = 0;
    int col_off = 0;
    int max_col_size=0;
    for( int i=0; i<num_cols; i++ ) {
      ValueArray.Column c= dp1._cols[i];
      int sz = Math.abs(c._size);
      // Dumb-ass in-order columns.  Later we should sort bigger columns first
      // to preserve 4 & 8-byte alignment for larger values
      c._off = (short)col_off;
      col_off += sz;
      row_size += sz;
      if( sz > max_col_size ) max_col_size = sz;
    }
    // Pad out the row to the max-aligned field
    row_size = (row_size+max_col_size-1)&~(max_col_size-1);

    // #5: Roll-up the rows-per-chunk.  Converts to: starting row# for this chunk.
    int rs[] = dp1._rows_chk;
    int rs2[] = new int[rs.length+1]; // One larger to hold final number of rows
    int off = 0;
    for( int i=0; i<rs.length; i++ ) {
      rs2[i] = off;
      off += rs[i];
    }
    rs2[rs.length] = off;

    // Setup for pass-2, where we do the actual data conversion. Also computes variance.
    DParse2 dp2 = new DParse2();
    dp2._num_cols  = num_cols;
    dp2._parseType = parseType;
    dp2._num_rows  = row_size; // Cheat: pass in rowsize instead of the num_rows (and its non-zero)
    dp2._cols      = dp1._cols;
    dp2._cols_domains = dp1._cols_domains;
    dp2._rows_chk  = rs2;       // Rolled-up row numbers
    dp2._result    = result;

    dp2.invoke(dataset._key);   // Parse whole dataset!


    // normalize the variance and turn it to sigma
    for( int i = 0; i < dp2._cols.length;++i )
      dp2._cols[i]._sigma = Math.sqrt(dp2._sumerr[i]/dp2._cols[i]._n);
    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "basic_parse", dp1._num_rows, row_size, dp2._cols);
    UKV.put(result,ary);

    // At this point we're left with a bunch of in-flight AtomicUnions for this
    // parse job.  They were all fired-and-forgotten, but they are not all done
    // yet.  We basically need a write-barrier here, where we block until all
    // pending writes are done.  So we're firing off a distributed job with the
    // main dataset Key again, and making each Node check for pending AU tasks
    // with this Key, and block until they are done.

    // As an alternative, we could gather the AU's up as we make them, and then
    // do some sort of bulk 'get' call on them all, blocking until they all
    // finished.  This has the downside of keeping all these AU's alive, along
    // with all their data until we "free" each one by down a get().

    // Plan A: distributed write barrier on atomic unions
    AUBarrier aub = new AUBarrier();
    aub.invoke(result);
    // Done building the result ValueArray!
  }

  // Unpack zipped CSV-style structure and call method parseUncompressed(...)
  // The method exepct a dataset which contains a ZIP file encapsulating one file.
  public static void parseZipped( Key result, Value dataset ) {
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
    } catch( IOException e ) {
      throw new Error(e);
    } finally { Closeables.closeQuietly(zis); }

    if( key!= null ) {
      Value uncompressedDataset = DKV.get(key);
      parse(result, uncompressedDataset);
      return ;
    }

    throw new Error("Cannot uncompressed ZIP-compressed dataset!");
  }

  public static void parseGZipped( Key result, Value dataset ) {
    GZIPInputStream gzis = null;
    Key key = null;
    try {
      gzis = new GZIPInputStream(dataset.openStream());
      key = ValueArray.read_put_stream(new String(dataset._key._kb) + "_UNZIPPED", gzis, Key.DEFAULT_DESIRED_REPLICA_FACTOR);
    } catch( IOException e ) {
      throw new Error(e);
    } finally { Closeables.closeQuietly(gzis); }

    if( key!= null ) {
      Value uncompressedDataset = DKV.get(key);
      parse(result, uncompressedDataset);
      return ;
    }

    throw new Error("Cannot uncompressed GZIP-compressed dataset!");
  }

  // ----
  private static void compute_column_size( DParse1 dp1) {
    int num_cols = dp1._cols.length;

    for( int i=0; i<num_cols; i++ ) {
      ValueArray.Column c= dp1._cols[i];
      if( c._min == Double.MAX_VALUE ) { // Column was only NaNs?  Skip column...
        c._size = 0;                     // Size is zero... funny funny column
        continue;
      }
      if( (c._size&31)==31 ) { // All fails; this is a plain double
        c._size = -8;          // Flag as a plain double
        continue;
      }
      if( (c._size&31)==15 ) { // All the int-versions fail, but fits in a float
        c._size = -4;          // Flag as a float
        continue;
      }
      // Else attempt something integer; try to squeeze into a short or byte.
      // First, scale to integers.
      if( (c._size & 8)==0 ) { c._scale = 1000; }
      if( (c._size & 4)==0 ) { c._scale =  100; }
      if( (c._size & 2)==0 ) { c._scale =   10; }
      if( (c._size & 1)==0 ) { c._scale =    1; }
      // Compute the scaled min/max
      double dmin = c._min*c._scale;
      double dmax = c._max*c._scale;
      long min = (long)dmin;
      long max = (long)dmax;
      assert min == dmin; // assert no truncation errors
      assert max == dmax;
      long spanl = (max-min);
      // Can I bias with a 4-byte int?
      if( min < Integer.MIN_VALUE || Integer.MAX_VALUE <= min ||
          max < Integer.MIN_VALUE || Integer.MAX_VALUE <= max ||
          ((int)spanl)!=spanl ) { // Span does not fit in an 'int'?
        // Nope; switch to large format
        c._size = (byte)((c._scale == 1) ? 8 : -8); // Either Long or Double
        continue;
      }
      int span = (int)spanl;

      // See if we fit in an unbiased byte, skipping 255 for missing values
      if( 0 <= min && max <= 254 ) { c._size = 1; continue; }
      // See if we fit in a  *biased* byte, skipping 255 for missing values
      if( span <= 254 ) { c._size = 1; c._base = (int)min; continue; }

      // See if we fit in an unbiased short, skipping 65535 for missing values
      if( 0 <= min && max <= 65534 ) { c._size = 2; continue; }
      // See if we fit in a  *biased* short, skipping 65535 for missing values
      if( span <= 65534 ) { c._size = 2; c._base = (int)min; continue; }
      // Must be an int, no bias needed.
      c._size = (byte)((c._scale == 1) ? 4 : -4); // Either int or float
    }
  }

  // True if the array is all NaNs
  private final static boolean allNaNs( double ds[] ) {
    for( double d : ds )
      if( !Double.isNaN(d) )
        return false;
    return true;
  }

  // Filter too big column enum domains and setup columns
  // which have enum domain.
  private static void filter_columns_domains( DParse1 dp1) {
    int num_col = dp1._num_cols;

    for( int i = 0; i < num_col; i++) {
      ColumnDomain colDom   = dp1._cols_domains[i];
      ValueArray.Column col = dp1._cols[i];
      col._domain = colDom;
      // Column domain contains column name => exclude column name from domain.
      if (colDom._domainValues.contains(col._name)) {
        colDom._domainValues.remove(col._name);
      }
      // The column's domain contains too many unique strings => drop the
      // domain. Column is already marked as erroneous.
      if( colDom.size() > ColumnDomain.DOMAIN_MAX_VALUES) colDom.kill();
      // Column domain was killed because it contains numbers => it does not
      // need to be considered any more
      if( !colDom.isKilled() ) {
        // column's domain is 'small' enough => setup the column to carry values 0..<domain size>-1
        // setup column sizes
        col._base  = 0;
        col._min   = 0;
        col._max   = colDom.size() - 1;
        col._badat = 0; // I can't precisely recognize the wrong column value => all cells contain correct value
        col._scale = 1; // Do not scale
        col._size  = 0; // Mark integer column
      }
    }
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

    public final boolean add(String s) {
      if (_killed) return false; // this column domain is not live anymore (too many unique values)
      if (_domainValues.size() == DOMAIN_MAX_VALUES) {
        kill();
        return false;
      }
      return _domainValues.add(s);
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

  // ----
  // Distributed parsing.

  // Just the common fields being moved over the wire during parse compaction.
  public static abstract class DParse extends MRTask {
    int _num_cols;                 // Input
    int _num_rows;                 // Output
    byte _parseType;               // Input, comma-separator
    ValueArray.Column _cols[];     // Column summary data
    int _rows_chk[];               // Rows-per-chunk
    ColumnDomain _cols_domains[];  // Input/Output - columns domains preserving insertion order (LinkedSet).

    // Hand-rolled serializer for the above common fields.
    // Some Day Real Soon auto-gen me.
    @Override
    public int wire_len() {
      assert _num_rows==0 || _cols != null;
      assert _num_rows==0 || _rows_chk != null;
      assert _num_rows==0 || _cols_domains != null;
      int colDomSize = 0;
      if( _cols_domains!=null )
        for( ColumnDomain cd : _cols_domains) colDomSize += cd.wire_len();
      return 4+4+1+(_num_rows==0?0:(_cols.length*ValueArray.Column.wire_len() + 4+_rows_chk.length*4))+colDomSize;
    }

    @Override
    public void write( Stream s ) {
      s.set4(_num_cols);
      s.set4(_num_rows);
      s.set1(_parseType);
      if( _num_rows == 0 ) return; // No columns?
      assert _cols.length == _num_cols;
      for( ValueArray.Column col : _cols )
        col.write(s);           // Yes columns; write them all
      // Now the rows-per-chunk array
      s.setAry4(_rows_chk);
      // Write all column domains.
      assert _cols_domains.length == _num_cols;
      for( ColumnDomain coldom : _cols_domains )
        coldom.write(s);
    }

    @Override
    public void write( DataOutputStream dos ) throws IOException {
      dos.writeInt(_num_cols);
      dos.writeInt(_num_rows);
      dos.writeByte(_parseType);
      if( _num_rows == 0 ) return; // No columns?
      assert _cols.length == _num_cols;
      for( ValueArray.Column col : _cols )
        col.write(dos);         // Yes columns; write them all
      // Now the rows-per-chunk array
      TCPReceiverThread.writeAry(dos,_rows_chk);
      // Write all column domains.
      assert _cols_domains.length == _num_cols;
      for( ColumnDomain coldom : _cols_domains )
        coldom.write(dos);
    }

    public void read( Stream s ) {
      _num_cols  = s.get4();
      _num_rows  = s.get4();
      _parseType = s.get1();
      if( _num_rows == 0 ) return; // No rows, so no cols
      assert _cols == null;
      _cols = new ValueArray.Column[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = ValueArray.Column.read(s);
      _rows_chk = s.getAry4();
      _cols_domains = new ColumnDomain[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols_domains[i] = ColumnDomain.read(s);
    }
    public void read( DataInputStream dis ) throws IOException {
      _num_cols  = dis.readInt();
      _num_rows  = dis.readInt();
      _parseType = dis.readByte();
      if( _num_rows == 0 ) return; // No rows, so no cols
      assert _cols == null;
      _cols = new ValueArray.Column[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = ValueArray.Column.read(dis);
      _rows_chk = TCPReceiverThread.readIntAry(dis);
      _cols_domains = new ColumnDomain[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols_domains[i] = ColumnDomain.read(dis);
    }
  }

  // ----
  // Distributed parsing, Pass 1
  // Find min/max, digits per column.  Find number of rows per chunk.
  // Collects columns' domains (in case the column contains string values).
  public static class DParse1 extends DParse {

    public void prepareForChunk() {
      assert _cols == null;
      _num_rows = 0;
      // A place to hold the column summaries
      _cols = new ValueArray.Column[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = new ValueArray.Column();
      _cols_domains =  new ColumnDomain[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols_domains[i] = new ColumnDomain();
    }

    public void addRow(Row row) {
      if( allNaNs(row._fieldVals) ) return; // Row is dead, skip it entirely

      // Row has some valid data, parse away
      if( row._fieldVals.length > _num_cols ){ // can happen only for svmlight format, enlarge the column array
        ValueArray.Column [] newCols = new ValueArray.Column[row._fieldVals.length];
        System.arraycopy(_cols, 0, newCols, 0, _cols.length);
        for(int i = _cols.length; i < newCols.length; ++i)
          newCols[i] = new ValueArray.Column();
        _cols = newCols;
        _num_cols = row._fieldVals.length;
      }

      ++_num_rows;
      for( int i = 0; i < row._fieldVals.length; ++i ) {
        double d = row._fieldVals[i];
        if(Double.isNaN(d)) { // Broken data on row
          _cols[i]._size |=32;  // Flag as seen broken data
          _cols[i]._badat = (char)Math.min(_cols[i]._badat+1,65535);
          continue;             // But do not muck up column stats
        }
        ++_cols[i]._n;
        // The column contains a number => mark column domain as dead.
        _cols_domains[i].kill();
        if( d < _cols[i]._min ) _cols[i]._min = d;
        if( d > _cols[i]._max ) _cols[i]._max = d;
        _cols[i]._mean += d;

        // I pass a flag in the _size field if any value is NOT an integer.
        if( ((int)(d     )) != (d     ) ) _cols[i]._size |= 1; // not int:      52
        if( ((int)(d*  10)) != (d*  10) ) _cols[i]._size |= 2; // not 1 digit:  5.2
        if( ((int)(d* 100)) != (d* 100) ) _cols[i]._size |= 4; // not 2 digits: 5.24
        if( ((int)(d*1000)) != (d*1000) ) _cols[i]._size |= 8; // not 3 digits: 5.239
        if( ((float)d)      !=  d       ) _cols[i]._size |=16; // not float   : 5.23912f
      }
    }

    public void finishChunk(int idx) {
      // Kill column domains which contain too many different values.
      for( ColumnDomain dict : _cols_domains ) {
        if (dict.size() > ColumnDomain.DOMAIN_MAX_VALUES) dict.kill();
      }

      // Also pass along the rows-per-chunk
      _rows_chk = new int[idx+1];
      _rows_chk[idx] = _num_rows;
      for( int i = 0; i < _num_cols; ++i ) _cols[i]._mean /= _cols[i]._n;
      if( _num_cols != _cols.length )
        _cols = Arrays.copyOfRange(_cols, 0, _num_cols);
    }

    // Parse just this chunk: gather min & max
    public void map( Key key ) {
      prepareForChunk();

      SeparatedValueParser csv = new SeparatedValueParser(key,
          _parseType == PARSE_COMMASEP ? ',' : ' ', _cols.length, _cols_domains);
      for( Row row : csv ) {
        addRow(row);
      }
      finishChunk(key.user_allowed() ? 0 : ValueArray.getChunkIndex(key));
    }

    // Combine results
    public void reduce( DRemoteTask rt ) {
      DParse1 dp = (DParse1)rt;
      _num_rows += dp._num_rows;
      _num_cols = Math.max(_num_cols, dp._num_cols);

      if( _cols == null ) {     // No local work?
        _cols = dp._cols;
      } else {
        if(_cols.length <= _num_cols){
          ValueArray.Column [] newCols = new ValueArray.Column[_num_cols];
          System.arraycopy(_cols, 0, newCols, 0, _cols.length);
          for(int i = _cols.length; i < _num_cols; ++i){
            newCols[i] = new ValueArray.Column();
          }
          _cols = newCols;
        }
        for( int i=0; i<dp._num_cols; i++ ) {
          ValueArray.Column c =    _cols[i];
          ValueArray.Column d = dp._cols[i];
          if( d._min < c._min ) c._min = d._min; // min of mins
          if( d._max > c._max ) c._max = d._max; // max of maxes
          if(c._n == d._n){
            c._mean = 0.5*(c._mean + d._mean);
          } else {
            double rc = (double)c._n/(c._n + d._n);
            double rd = (double)d._n/(c._n + d._n);
            c._mean = rc*c._mean + rd*d._mean;
          }
          c._n += d._n;
          c._size |=  d._size;                   // accumulate size fail bits
          _cols[i]._badat = (char)Math.min(_cols[i]._badat+d._badat,65535);
        }
      }

      // Also roll-up the rows-per-chunk info.
      int r1[] =    _rows_chk;
      int r2[] = dp._rows_chk;
      // Grab the larger array.
      if( r1 == null ) {        // No local work?
        r1 = r2;
      } else {
        if( r1.length < r2.length ) { r1 = r2; r2 = _rows_chk; }
        // Copy smaller into larger, keeping non-zero numbers
        for( int i=0; i<r2.length; i++ )
          r1[i] += r2[i];
      }
      _rows_chk = r1;

      // Also merge column dictionaries
      ColumnDomain d1[] =    _cols_domains;
      ColumnDomain d2[] = dp._cols_domains;
      if ( d1 == null) {
        d1 = d2;
      } else {
        if (d1.length < d2.length) {
          Set<String> newD1[] = new Set[d2.length];
          System.arraycopy(d1, 0, newD1, 0, d1.length);
        }
        // Union of d1 and d2 but preserve the insertion order.
        for (int i = 0; i < d1.length; i++) {
          if (d1[i] == null) {
            d1[i] = d2[i];
          } else if (d2[i] != null) { // Insert domain elements from d2 into d1.
            d1[i] = d1[i].union(d2[i]);
          }
        }
      }
      _cols_domains = d1;

      // clean-up
      dp._cols         = null;
      dp._rows_chk     = null;
      dp._cols_domains = null;
    }
  }

  // ----
  // Distributed parsing, Pass 2
  // Parse the data, and jam it into compressed fixed-sized row data
  public static class DParse2 extends DParse {
    Key _result;                // The result Key
    double _sumerr[];

    // Parse just this chunk, compress into new format.
    public void map( Key key ) {
      assert _cols != null;
      assert _num_rows != 0;
      int row_size = _num_rows; // Cheat: row_size instead of num_rows
      // Get chunk index
      long cidx = key.user_allowed() ? 0 : ValueArray.getChunkIndex(key);
      // Get the starting row of the source chunk
      long start_row = _rows_chk[(int)cidx];
      // Get number of rows in this source chunk
      int num_rows = (int)(_rows_chk[(int)cidx+1]-start_row);
      // Get a place to hold the data
      byte[] buf = MemoryManager.allocateMemory(num_rows*row_size);
      // A place to hold each column datum
      _sumerr = new double[_num_cols];
      // The parser
      SeparatedValueParser csv = new SeparatedValueParser(key,
          _parseType == PARSE_COMMASEP ? ',' : ' ', _cols.length);
      // Prepare hashmap for each column domain to get domain item's index quickly
      HashMap<String,Integer> columnIndexes[] = new HashMap[_num_cols];
      for (int i = 0; i < _num_cols; i++) {
        if (_cols_domains[i].size() == 0) continue;
        columnIndexes[i] = new HashMap<String, Integer>();
        int j = 0;
        for (String s : _cols_domains[i]._domainValues) {
          columnIndexes[i].put(s,j++);
        }
      }

      // Fill the rows
      int off = 0;
      for( Row row : csv ) {
        if( allNaNs(row._fieldVals) ) continue; // Row is dead, skip it entirely
        int old = off;
        for( int i=0; i<_cols.length; i++ ) {
          double d = row._fieldVals[i];
          ValueArray.Column col = _cols[i];
          if ( columnIndexes[i] != null) {
            assert Double.isNaN(d);
            d = columnIndexes[i].get(row._fieldStringVals[i]).intValue();
          }
          // Write to compressed values
          if( !Double.isNaN(d) ) { // Broken data on row?
            _sumerr[i] += (col._mean - d) * (col._mean - d);
            switch( col._size ) {
            case  1: buf[off++] = (byte)(d*col._scale-col._base); break;
            case  2: UDP.set2 (buf,(off+=2)-2, (int)(d*col._scale-col._base)); break;
            case  4: UDP.set4 (buf,(off+=4)-4, ( int)d); break;
            case  8: UDP.set8 (buf,(off+=8)-8, (long)d); break;
            case -4: UDP.set4f(buf,(off+=4)-4,(float)d); break;
            case -8: UDP.set8d(buf,(off+=8)-8,       d); break;
            }
          } else {
            switch( col._size ) {
            case  1: buf[off++] = (byte)-1; break;
            case  2: UDP.set2 (buf,(off+=2)-2,              -1 ); break;
            case  4: UDP.set4 (buf,(off+=4)-4,Integer.MIN_VALUE); break;
            case  8: UDP.set8 (buf,(off+=8)-8,   Long.MIN_VALUE); break;
            case -4: UDP.set4f(buf,(off+=4)-4,        Float.NaN); break;
            case -8: UDP.set8d(buf,(off+=8)-8,       Double.NaN); break;
            }
          }
        }
        off = old+row_size;     // Skip the padding during fill
      }

      // Compute the last dst chunk (which might be up to 2meg instead of capped at 1meg)
      long max_row = _rows_chk[_rows_chk.length-1];
      int rpc = (int)(ValueArray.chunk_size()/row_size); // Rows per chunk
      long dst_chks = max_row/rpc;

      _rows_chk = null;
      _cols_domains = null;

      // Now, rather painfully, ship the bits to the target keys.  Ship in
      // large chunks according to what fits in the next target chunk.
      int row0 = 0;             // Number of processed rows
      while( (row0 += atomic_update( row0, start_row, row_size, num_rows, buf, rpc, dst_chks )) < num_rows ) ;

      _num_rows = 0;            // No data to return
    }

    public void reduce( DRemoteTask rt ) {
      // return the variance
      DParse2 other = (DParse2)rt;
      if( _sumerr == null )
        _sumerr = other._sumerr;
      else {
        for(int i = 0; i < _cols.length; ++i)
          _sumerr[i] += other._sumerr[i];
      }
    }

    // Atomically fold together as many rows as will fit in the next chunk.  I
    // have an array of bits (buf) which is an even count of rows.  I want to
    // pack them into the target ValueArray, as many as will fit in a next
    // chunk.  Because the size isn't an even multiple of chunks, I surely will
    // need to update multiple target chunks.  (imagine parallel copying a
    // large source buffer into a chunked target buffer)
    int atomic_update( int row0, long start_row, int row_size, int num_rows, byte[] buf, int rpc, long dst_chks ) {
      assert 0 <= row0 && row0 <= num_rows;
      assert buf.length == num_rows*row_size;

      int src_off = row0*row_size; // Offset in buf to write from
      long row1 = start_row+row0;  // First row to write to
      long chk1 = row1/rpc;        // First chunk to write to
      if( chk1 > 0 && chk1 == dst_chks ) // Last chunk?
        chk1--;                 // It's actually the prior chunk, made bigger
      // Get the key for that chunk.  Note that this key may not yet exist.
      Key key1 = ValueArray.make_chunkkey(_result,ValueArray.chunk_offset(chk1));
      // Get the starting row# for this chunk
      long row_s = chk1*rpc;
      // Get the number of rows to skip
      int rowx = (int)(row1-row_s); // This is the row# we start writing in this chunk
      int dst_off = rowx*row_size; // Offset in the dest chunk
      // Rows to write in this chunk
      int rowy = rpc - rowx;      // Number of rows we could write in a 1meg chunk
      int rowz = num_rows - row0; // Number of unwritten rows in our source
      if( chk1 < dst_chks-1 && rowz > rowy ) // Not last chunk (which is large) and more rows
        rowz = rowy;              // Limit of rows to write
      int len = rowz*row_size;    // Bytes to write

      assert src_off+len <= buf.length;

      // Remotely, atomically, merge this buffer into the remote key
      AtomicUnion au = new AtomicUnion(buf,src_off,dst_off,len);
      au.fork(key1);            // Start atomic update
      // Do not wait on completion now; the atomic-update is fire-and-forget.
      //au.get();               // No need to complete now?
      return rowz;              // Rows written out
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
        byte[] bits2 = (bits1 == null)
          ? new byte[_dst_off + mem.length] // Initial array of correct size
          : Arrays.copyOf(bits1,Math.max(_dst_off+mem.length,bits1.length));
        System.arraycopy(mem,0,bits2,_dst_off,mem.length);
        return bits2;
      }

      @Override public void onSuccess() {
        DKV.remove(_key);
      }
    }
  }

  // ----
  // Distributed blocking for all the pending AUs to complete.
  public static class AUBarrier extends DRemoteTask {
    // Basic strategy is to check all the Nodes in the cloud to see if they
    // have any pending not-yet-completed AtomicUnions to the correct key.  If
    // so, block until they complete.
    public final void compute() {
      Key abkey = _keys[0];
      if( abkey == null ) { tryComplete(); return; } // No AU's here
      byte[] abb = ValueArray.getArrayKeyBytes(abkey);
      for( DFutureTask dft : DFutureTask.TASKS.values() ) { // For all active tasks
        if( dft instanceof TaskRemExec ) {                  // See if it's a TRE
          TaskRemExec tre = (TaskRemExec)dft;
          RemoteTask rt = tre._dt; // Get what is pending execution
          if( rt instanceof DParse2.AtomicUnion ) { // See if its an AtomicUnion
            DParse2.AtomicUnion au = (DParse2.AtomicUnion)rt;
            Key aukey = ((Atomic)au)._key; // Get the AtomicUnion's transaction key
            byte[] aub = ValueArray.getArrayKeyBytes(aukey);
            if( Arrays.equals(abb,aub) ) // See if it matches OUR key
              tre.get();        // Block for the AtomicUnion to complete
          }
        }
      }
      tryComplete();            // All done...
    }
    public void reduce( DRemoteTask drt ) { }
  }


  // Guess
  private static Compression guess_compression_method(Value dataset) {
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

    if( b.length > XLS_MAGIC_OFFSET + XLS_MAGIC.length &&
        Arrays.equals(XLS_MAGIC, Arrays.copyOfRange(b, XLS_MAGIC_OFFSET, XLS_MAGIC.length))) {
      return new int[] { PARSE_EXCEL, 0 };
    }
    if( b.length > XLSX_MAGIC_OFFSET + XLSX_MAGIC.length &&
        Arrays.equals(XLSX_MAGIC, Arrays.copyOfRange(b, XLSX_MAGIC_OFFSET, XLSX_MAGIC.length))) {
      return new int[] { PARSE_EXCEL, 0 };
    }

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
    return new int[]{commas ? PARSE_COMMASEP : PARSE_SPACESEP, cols};
  }

  // ---

  // Alternative column title guesser.  Returns an array of Strings, or
  // null if none.
  private static String[] guess_col_names( Value dataset, int num_cols, byte csvType ) {
    Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
    byte[] b = v0.get();                      // Bytes for 1st chunk
    String [] names = new String[num_cols];
    int cols=0;
    int idx=-1;
    int num = 0;
    boolean escaped = false;
    for( int i=0; i<b.length; i++ ) {
      char c = (char)b[i];
      if (c=='"') {
        escaped = !escaped;
        if ( idx == -1) {
          cols++;
          idx=i;
        }
        continue;
      }
      if (!escaped) {
        if( c=='\n' || c== '\r' ) {
          if( cols > 0 && (names[cols-1] = NaN(b,idx,i)).length() > 0 )
            num++;                // Not a number, so take it as a column name
          break;
        }
        // Found a column separator?
        if(    (csvType==PARSE_SPACESEP && Character.isWhitespace(c) && idx!=-1)
            || (csvType==PARSE_COMMASEP && c == ',') ) {
          if( cols > 0 && (names[cols-1] = NaN(b,idx,i)).length() > 0 )
            num++;                // Not a number, so take it as a column name
          idx = -1;               // Reset start-of-column
        } else if (csvType==PARSE_SPACESEP && Character.isWhitespace(c)) {
          // Skip multiple whitespaces in space separated mode
        } else {                  // Else its just column data
          if( idx == -1 ) {       // Not starting a name?
            cols++;               // Starting a name now
            idx = i;
          }
        }
      }
    }
    // Claim some column names if the minority look like numbers
    return (num > (num_cols>>2)) ? names : null;
  }

  private static String NaN( byte[] b, int idx, int i ) {
    if( idx == -1 ) return "";
    String s = new String(b,idx,i-idx).trim();
    try { Double.parseDouble(s); return ""; } catch( NumberFormatException e ) { }
    try { Long  .parseLong  (s); return ""; } catch( NumberFormatException e ) { }
    return s;
  }
}
