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
    int [] typeArr = guessParserSetup(dataset,false);
    switch( typeArr[PARSER_IDX] ) {
    case PARSE_SVMLIGHT:
      throw new Error("Parsing for SVMLIGHT is unimplemented");
    case PARSE_EXCEL:
      parseExcel(result, dataset);
      break;
    case PARSE_COMMASEP:
    case PARSE_SPACESEP:
      parseSeparated(result, dataset, (byte) typeArr[PARSER_IDX], typeArr[COLNUM_IDX]);
      break;
    default: H2O.unimpl();
    }

  }

  public static void parseExcel(Key result, Value dataset) throws IOException {
    final ParseState state = new ParseState();
    state._num_rows  = 0; // No rows yet
    state._num_cols  = 0; // No rows yet

    ExcelParser parser = new ExcelParser(dataset.openStream()) {
      @Override
      public void handleRow(double[] rowNums, String[] rowStrs) {
        if( state._num_cols == 0 ) {
          state._num_cols = rowNums.length;
          state.prepareForStatsGathering();
        }
        if( allNaNs(rowNums) ) return;
        state.addRowToStats(new Row(rowNums, rowStrs));
      }
    };

    parser.process();
    state.finishStatsGathering(0);

    String[] names = parser._firstRow;
    int num = 0;
    for( String n : names ) if( n != null ) ++num;
    names = num > names.length/2 ? names : null;
    state.assignColumnNames(names);
    state.filterColumnsDomains();
    state.computeColumnSize();

    final int num_cols = state._num_cols;
    final int num_rows = state._num_rows;
    final int row_size = state.computeOffsets();
    final byte[] buf = MemoryManager.allocateMemory(num_rows*row_size);
    final double[] sumerr = new double[num_cols];
    final HashMap<String,Integer> columnIndexes[] = state.createColumnIndexes();
    parser = new ExcelParser(dataset.openStream()) {
      int off = 0;
      @Override
      public void handleRow(double[] rowNums, String[] rowStrs) {
        if( allNaNs(rowNums) ) return;
        state.addRowToBuffer(buf, off, sumerr, new Row(rowNums, rowStrs), columnIndexes);
        off += row_size;
      }
    };
    parser.process();

    // normalize the variance and turn it to sigma
    for( int i = 0; i < state._cols.length;++i )
      state._cols[i]._sigma = Math.sqrt(sumerr[i]/state._cols[i]._n);

    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "excel_parse",
        num_rows, row_size, state._cols);
    final byte[] base = ary.get();
    byte[] res = MemoryManager.allocateMemory(base.length + buf.length);
    System.arraycopy(base, 0, res, 0, base.length);
    System.arraycopy(buf, 0, res, base.length, buf.length);
    DKV.put(result, new Value(result, res));
  }

  public static void parseSeparated(Key result, Value dataset, byte parseType, int num_cols) {

    DParseStatsPass dp1 = new DParseStatsPass();
    dp1._parseType = parseType;
    ParseState state = dp1._state;
    state._num_cols  = num_cols;
    state._num_rows  = 0; // No rows yet
    dp1.invoke(dataset._key); // Parse whole dataset!
    assert state == dp1._state;

    num_cols = state._num_cols;
    int num_rows = state._num_rows;

    // #1: Guess Column names, if any
    String[] names = null;
    try { // to be robust here have a fail-safe mode but log the exception
          // NOTE: we should log such fail-safe situations
      names = guessColNames(dataset, num_cols, parseType);
    } catch( Exception e ) {
      System.err.println("[parser] Column names guesser failed.");
      e.printStackTrace(System.err);
    }
    state.assignColumnNames(names);

    // #2: Filter columns which are too big (contains too many unique strings)
    // and manage domain overlap with column name.
    state.filterColumnsDomains();

    // #3: Now figure out how best to represent the data.
    state.computeColumnSize();

    // #4: Compute row size & column offsets
    int row_size = state.computeOffsets();

    // Setup for pass-2, where we do the actual data conversion. Also computes variance.
    DParseCollectDataPass dp2 = new DParseCollectDataPass();
    dp2._parseType = parseType;
    dp2._row_size = row_size;
    dp2._state  = state;
    dp2._result = result;
    dp2.invoke(dataset._key);   // Parse whole dataset!
    assert state == dp2._state;


    // normalize the variance and turn it to sigma
    for( int i = 0; i < state._cols.length; ++i )
      state._cols[i]._sigma = Math.sqrt(dp2._sumerr[i]/state._cols[i]._n);
    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "basic_parse", state._num_rows, row_size, state._cols);
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
          _state._cols.length, _state._cols_domains);
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
      assert _state._num_rows != 0;

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

      // Compute the last dst chunk (which might be up to 2meg instead of capped at 1meg)
      long max_row = _state._rows_chk[_state._rows_chk.length-1];
      int rpc = (int)(ValueArray.chunk_size()/_row_size); // Rows per chunk
      long dst_chks = max_row/rpc;

      // Reset these arrays to null, so they are not part of the return result.
      _state._rows_chk = null;
      _state._cols = null;
      _state._cols_domains = null;
      _state._num_rows = 0;            // No data to return

      // Now, rather painfully, ship the bits to the target keys.  Ship in
      // large chunks according to what fits in the next target chunk.
      int row0 = 0;             // Number of processed rows
      while( (row0 += atomic_update( row0, start_row, _row_size, num_rows, buf, rpc, dst_chks )) < num_rows ) ;
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
          if( rt instanceof DParseCollectDataPass.AtomicUnion ) { // See if its an AtomicUnion
            DParseCollectDataPass.AtomicUnion au = (DParseCollectDataPass.AtomicUnion)rt;
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

    if( b.length > XLS_MAGIC_OFFSET + XLS_MAGIC.length &&
        Arrays.equals(XLS_MAGIC,
            Arrays.copyOfRange(b, XLS_MAGIC_OFFSET, XLS_MAGIC_OFFSET + XLS_MAGIC.length))) {
      return new int[] { PARSE_EXCEL, 0 };
    }
    if( b.length > XLSX_MAGIC_OFFSET + XLSX_MAGIC.length &&
        Arrays.equals(XLSX_MAGIC,
            Arrays.copyOfRange(b, XLSX_MAGIC_OFFSET, XLSX_MAGIC_OFFSET + XLSX_MAGIC.length))) {
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
    return new int[]{ commas ? PARSE_COMMASEP : PARSE_SPACESEP, cols };
  }

  // ---

  // Alternative column title guesser.  Returns an array of Strings, or
  // null if none.
  private static String[] guessColNames( Value dataset, int num_cols, byte csvType ) {
    SeparatedValueParser csv = new SeparatedValueParser(dataset._key,
        csvType == PARSE_COMMASEP ? ',' : ' ', num_cols);
    Iterator<Row> it = csv.iterator();
    if( !it.hasNext() ) return null;
    Row r = it.next();

    String[] names = r._fieldStringVals;
    int num = 0;
    for( String n : names ) if( n != null ) ++num;
    return num > num_cols/2 ? names : null;
  }
}
