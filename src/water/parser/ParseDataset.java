package water.parser;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.hyperic.sigar.Mem;

import com.google.common.primitives.Chars;

import water.*;
import water.serialization.RTSerializer;
import water.serialization.RemoteTaskSerializer;
import water.ValueArray.Column;

// Helper class to parse an entire ValueArray data, and produce a structured
// ValueArray result.
//
// @author <a href="mailto:cliffc@0xdata.com"></a>

public final class ParseDataset {
  static final int PARSE_SVMLIGHT = 101;
  static final int PARSE_COMMASEP = 102;
  static final int PARSE_SPACESEP = 103; 

  // Parse the dataset as a CSV-style thingy and produce a structured dataset
  // result.  This does a distributed parallel parse.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");

    // Guess on the number of columns, build a column array.
    int [] typeArr = guess_parser_setup(dataset);
    int num_cols = typeArr[1];

    DParse1 dp1 = new DParse1();
    dp1._num_cols  = num_cols;
    dp1._parseType = (byte)typeArr[0];
    dp1._num_rows  = 0; // No rows yet
    
    dp1.invoke(dataset._key);   // Parse whole dataset!

    num_cols = dp1._num_cols;
    // Now figure out how best to represent the data.
    compute_column_size(dp1);

    // Compute row size & column offsets
    int row_size = 0;
    int col_off = 0;
    int max_col_size=0;
    for( int i=0; i<num_cols; i++ ) {
      ValueArray.Column c= dp1._cols[i];
      c._name = ""; // No column names for now
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

    // Roll-up the rows-per-chunk.  Converts to: starting row# for this chunk.
    int rs[] = dp1._rows_chk;
    int rs2[] = new int[rs.length+1]; // One larger to hold final number of rows
    int off = 0;
    for( int i=0; i<rs.length; i++ ) {
      rs2[i] = off;
      off += rs[i];
    }
    rs2[rs.length] = off;

    // Column names, if any
    String[] names = guess_col_names(dataset, num_cols, (byte)typeArr[0]);
    if( names != null )
      for( int i=0; i<num_cols; i++ )
        dp1._cols[i]._name = names[i];

    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, Value.ICE, dataset._key, "basic_parse", dp1._num_rows, row_size, dp1._cols);
    UKV.put(result,ary);

    // Setup for pass-2, where we do the actual data conversion.
    DParse2 dp2 = new DParse2();
    dp2._num_cols  = num_cols;
    dp2._parseType = (byte)typeArr[0];
    dp2._num_rows  = row_size; // Cheat: pass in rowsize instead of the num_rows (and its non-zero)
    dp2._cols      = dp1._cols;
    dp2._rows_chk  = rs2;        // Rolled-up row numbers
    dp2._result    = result;

    dp2.invoke(dataset._key);   // Parse whole dataset!

    // Done building the result ValueArray!
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
      assert (double)min == dmin; // assert no truncation errors
      assert (double)max == dmax;
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

  // ----
  // Distributed parsing.

  // Just the common fields being moved over the wire during parse compaction.
  public static abstract class DParse extends MRTask {
    int _num_cols;              // Input
    int _num_rows;              // Output
    byte _parseType;            // Input, comma-separator
    ValueArray.Column _cols[];  // Column summary data
    int _rows_chk[];            // Rows-per-chunk
  }

  // Hand-rolled serializer for the above common fields.
  // Some Day Real Soon auto-gen me.
  public static abstract class DParseSerializer<T extends DParse> extends RemoteTaskSerializer<T> {
    public int wire_len(T dp) {
      assert dp._num_rows==0 || dp._cols != null;
      assert dp._num_rows==0 || dp._rows_chk != null;
      return 4+4+1+(dp._num_rows==0?0:(dp._cols.length*ValueArray.Column.wire_len() + 4+dp._rows_chk.length*4));
    }

    public int write( T dp, byte[] buf, int off ) {
      UDP.set4(buf,(off+=4)-4,dp._num_cols);
      UDP.set4(buf,(off+=4)-4,dp._num_rows);
      buf[off++] = dp._parseType;
      if( dp._num_rows == 0 ) return off; // No columns?
      assert dp._cols.length == dp._num_cols;
      for( ValueArray.Column col : dp._cols )
        off = col.write(buf,off); // Yes columns; write them all
      // Now the rows-per-chunk array
      UDP.set4(buf,(off+=4)-4,dp._rows_chk.length);
      for( int x : dp._rows_chk )
        UDP.set4(buf,(off+=4)-4,x);
      return off;
    }
    public void write( T dp, DataOutputStream dos ) throws IOException {
      dos.writeInt(dp._num_cols);
      dos.writeInt(dp._num_rows);
      dos.writeByte(dp._parseType);
      if( dp._num_rows == 0 ) return; // No columns?
      assert dp._cols.length == dp._num_cols;
      for( ValueArray.Column col : dp._cols )
        col.write(dos);         // Yes columns; write them all
      // Now the rows-per-chunk array
      dos.writeInt(dp._rows_chk.length);
      for( int x : dp._rows_chk )
        dos.writeInt(x);
    }

    public abstract T read( byte[] buf, int off );
    public T read( T dp, byte[] buf, int off ) {
      dp._num_cols  = UDP.get4(buf,off);  off += 4;
      dp._num_rows  = UDP.get4(buf,off);  off += 4;
      dp._parseType = buf[off++];
      if( dp._num_rows == 0 ) return dp; // No rows, so no cols
      assert dp._cols == null;
      dp._cols = new ValueArray.Column[dp._num_cols];
      final int l = ValueArray.Column.wire_len();
      for( int i=0; i<dp._num_cols; i++ )
        dp._cols[i] = ValueArray.Column.read(buf,(off+=l)-l);
      int rlen = UDP.get4(buf,off);  off += 4;
      dp._rows_chk = new int[rlen];
      for( int i=0; i<rlen; i++ )
        dp._rows_chk[i] = UDP.get4(buf,(off+=4)-4);
      return dp;
    }
    public abstract T read( DataInputStream dis ) throws IOException;
    public T read( T dp, DataInputStream dis ) throws IOException {
      dp._num_cols  = dis.readInt();
      dp._num_rows  = dis.readInt();
      dp._parseType = dis.readByte();
      if( dp._num_rows == 0 ) return dp; // No rows, so no cols
      assert dp._cols == null;
      dp._cols = new ValueArray.Column[dp._num_cols];
      for( int i=0; i<dp._num_cols; i++ )
        dp._cols[i] = ValueArray.Column.read(dis);
      int rlen = dis.readInt();
      dp._rows_chk = new int[rlen];
      for( int i=0; i<rlen; i++ )
        dp._rows_chk[i] = dis.readInt();
      return dp;
    }
  }

  // ----
  // Distributed parsing, Pass 1
  // Find min/max, digits per column.  Find number of rows per chunk.
  @RTSerializer(DParse1.Serializer.class)
  public static class DParse1 extends DParse {
    public static class Serializer extends DParseSerializer<DParse1> {
      public DParse1 read( byte[] buf, int off ) { return read(new DParse1(), buf, off ); }
      public DParse1 read( DataInputStream dis ) throws IOException { return read(new DParse1(), dis ); }
    }

    // Parse just this chunk: gather min & max
    public void map( Key key ) {
      assert _cols == null;
      // A place to hold the column summaries
      _cols = new ValueArray.Column[_num_cols];
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = new ValueArray.Column();
      // A place to hold each column datum
      double[] data = new double[_num_cols];
      // The parser
      if( _parseType == PARSE_SVMLIGHT ) throw new Error("SVMLIGHT is unimplemented");
      CSVParserKV<double[]> csv = new CSVParserKV<double[]>(key,1,data,null);
      csv._setup.whiteSpaceSeparator = (_parseType == PARSE_SPACESEP);

      // Parse row-by-row until the whole file is parsed
      int num_rows = 0;
      for( double[] ds : csv ) {
        if( allNaNs(ds) ) continue; // Row is dead, skip it entirely
        // Row has some valid data, parse away
        if(ds.length > _num_cols){ // can happen only for svmlight format, enlarge the column array
          Column [] newCols = new Column[ds.length]; 
          System.arraycopy(_cols, 0, newCols, 0, _cols.length);
          for(int i = _cols.length; i < newCols.length; ++i)
            newCols[i] = new Column();
          _cols = newCols;
          _num_cols = csv.ncolumns();
        }
        num_rows++;
        for( int i=0; i<ds.length; i++ ) {
          double d = ds[i];
          if(Double.isNaN(d)) { // Broken data on row
            _cols[i]._size |=32;  // Flag as seen broken data
            _cols[i]._badat = Chars.saturatedCast(_cols[i]._badat + 1);
            continue;             // But do not muck up column stats
          }
          if( d < _cols[i]._min ) _cols[i]._min = d;
          if( d > _cols[i]._max ) _cols[i]._max = d;
          // I pass a flag in the _size field if any value is NOT an integer.
          if( (double)((int)(d     )) != (d     ) ) _cols[i]._size |= 1; // not int:      52
          if( (double)((int)(d*  10)) != (d*  10) ) _cols[i]._size |= 2; // not 1 digit:  5.2
          if( (double)((int)(d* 100)) != (d* 100) ) _cols[i]._size |= 4; // not 2 digits: 5.24
          if( (double)((int)(d*1000)) != (d*1000) ) _cols[i]._size |= 8; // not 3 digits: 5.239
          if( (double)((float)d)      !=  d       ) _cols[i]._size |=16; // not float   : 5.23912f
        }
      }
      
      assert num_rows > 0;   // Parsing no rows generally means a broken parser
      _num_rows = num_rows;
      // Also pass along the rows-per-chunk
      int idx = key.user_allowed() ? 0 : ValueArray.getChunkIndex(key);
      _rows_chk = new int[idx+1];
      _rows_chk[idx] = num_rows;
      if(_num_cols != _cols.length){
        _cols = Arrays.copyOfRange(_cols, 0, _num_cols);
      }
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
          Column [] newCols = new Column[_num_cols];
          System.arraycopy(_cols, 0, newCols, 0, _cols.length);
          for(int i = _cols.length; i < _num_cols; ++i){
            newCols[i] = new Column();
          }
          _cols = newCols;
        }
        for( int i=0; i<dp._num_cols; i++ ) {
          ValueArray.Column c =    _cols[i];
          ValueArray.Column d = dp._cols[i];
          if( d._min < c._min ) c._min = d._min; // min of mins
          if( d._max > c._max ) c._max = d._max; // max of maxes
          c._size |=  d._size;                   // accumulate size fail bits
          c._badat = Chars.saturatedCast(c._badat+d._badat);
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
      dp._cols = null;
      dp._rows_chk = null;
    }
  }

  // ----
  // Distributed parsing, Pass 2
  // Parse the data, and jam it into compressed fixed-sized row data
  @RTSerializer(DParse2.Serializer.class)
  public static class DParse2 extends DParse {
    Key _result;                // The result Key

    public static class Serializer extends DParseSerializer<DParse2> {
      // Pass along the result Key
      public int wire_len(DParse2 dp) {
        return super.wire_len(dp)+dp._result.wire_len();
      }
      public int write( DParse2 dp, byte[] buf, int off ) {
        off = super.write(dp,buf,off);
        off += dp._result.write(buf,off);
        return off;
      }
      public DParse2 read( byte[] buf, int off ) {
        DParse2 dp = new DParse2();
        super.read(dp,buf,off);
        off += super.wire_len(dp);
        dp._result = Key.read(buf,off);
        assert dp._result != null;
        return dp;
      }
      public void write( DParse2 dp, DataOutputStream dos ) throws IOException {
        super.write(dp,dos);
        dp._result.write(dos);
      }
      public DParse2 read( DataInputStream dis ) throws IOException {
        DParse2 dp = new DParse2();
        super.read(dp,dis);
        dp._result = Key.read(dis);
        return dp;
      }
    }

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
      // The parser
      CSVParserKV<double[]> csv = new CSVParserKV<double[]>(key,1,new double[_cols.length],null);
      csv._setup.whiteSpaceSeparator = (_parseType == PARSE_SPACESEP);
      // Fill the rows
      int off = 0;
      for( double[] ds : csv ) {
        if( allNaNs(ds) ) continue; // Row is dead, skip it entirely
        int old = off;
        for( int i=0; i<_cols.length; i++ ) {
          double d = ds[i];
          ValueArray.Column col = _cols[i];
          if( !Double.isNaN(d) ) { // Broken data on row?
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

      _cols = null;             // Free for GC purposes before I/O begins
      _rows_chk = null;

      // Now, rather painfully, ship the bits to the target keys.  Ship in
      // large chunks according to what fits in the next target chunk.
      int row0 = 0;             // Number of processed rows
      while( (row0 += atomic_update( row0, start_row, row_size, num_rows, buf, rpc, dst_chks )) < num_rows ) ;
      
      _num_rows = 0;            // No data to return
    }

    public void reduce( DRemoteTask rt ) {
      _num_cols = 0;            // No data to return
      _num_rows = 0;            // No data to return
      _cols = null;             // No data to return
      _rows_chk = null;
    }

    // Atomically fold together as many rows as will fit in the next chunk.  I
    // have an array of bits (buf) which is an even count of rows.  I want to
    // pack them into the target ValueArray, as many as will fit in a next
    // chunk.  Because the size isn't an even multiple of chunks, I surely will
    // need to update multiple target chunks.  (imagine parallel copying a
    // large source buffer into a chunked target buffer)
    int atomic_update( int row0, long start_row, int row_size, int num_rows, byte[] buf, int rpc, long dst_chks ) {
      assert 0 <= row0 && row0 < num_rows;
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

    @RTSerializer(AtomicUnion.Serializer.class)
    public static class AtomicUnion extends Atomic {
    public static class Serializer extends RemoteTaskSerializer<AtomicUnion> {
      // By default, nothing sent over with the function (except the target Key).
      @Override public int  wire_len(AtomicUnion a) { return 4+a._key._kb.length; }
      @Override public int  write( AtomicUnion a, byte[] buf, int off ) {
        off += UDP.set4(buf,off,a._dst_off);
        return a._key.write(buf, off);        
      }
      @Override public void write( AtomicUnion a, DataOutputStream dos ) throws IOException {
        dos.writeInt(a._dst_off);
        a._key.write(dos);
      }
      @Override public AtomicUnion read( byte[] buf, int off ) {
        AtomicUnion a = new AtomicUnion();
        a._dst_off = UDP.get4(buf,(off+=4)-4);        
        a._key = Key.read(buf, off);                
        return a;
      }
      @Override public AtomicUnion read( DataInputStream dis ) throws IOException {
        AtomicUnion a = new AtomicUnion();
        a._dst_off = dis.readInt();
        a._key = Key.read(dis);        
        return a;
      }
    }
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
  
  // ---
  // Guess type of file (csv comma separated, csv space separated, svmlight) and the number of columns,
  // the number of columns for svm light is not reliable as it only relies on info from the first chunk
  private static int[] guess_parser_setup(Value dataset) {
    // Best-guess on count of columns and separator.  Skip the 1st line.  
    // Count column delimiters in the next line. If there are commas, assume file is comma separated.
    // if there are (several) ':', assume it is in svmlight format.
    Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
    byte[] b = v0.get();                      // Bytes for 1st chunk
    int i=0;
    while( i<b.length && b[i] != '\r' && b[i] != '\n' ) i++;   // Skip a line
    if( i==b.length ) return new int[]{0,0};  // No columns?
    if( b[i] == '\r' || b[i+1]=='\n' ) i++;
    if( b[i] == '\n' ) i++;
    // start counting columns on the 2nd line
    final int line_start = i;
    int cols = 0;
    int mode = 0;
    int colonCounter = 0;
    boolean commas = false;     // Assume white-space only columns
    while( true ) {
      char c = (char)b[i++];
      if( c=='\n' || c== '\r' ) {
        break;
      } if( !commas && Character.isWhitespace(c) ) { // Whites-space column seperator
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
      } else if( c == '"' ) {
        throw new Error("string skipping not implemented");
      } else if(c == ':' && (++colonCounter == 3)){
        // if there are at least 3 ':' on the line, the file is probably svmlight format
        throw new Error("SVMLIGHT format is currently unsupported");
      } else {                  // Else its just column data
        if( mode != 1 ) cols++;
        mode = 1;
      }
    }
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
    for( int i=0; i<b.length; i++ ) {
      char c = (char)b[i];
      if( c=='\n' || c== '\r' ) {
        if( cols > 0 && (names[cols-1] = NaN(b,idx,i)).length() > 0 )
          num++;                // Not a number, so take it as a column name
        break;
      }
      // Found a column separator?
      if(    (csvType==PARSE_SPACESEP && Character.isWhitespace(c))
          || (csvType==PARSE_COMMASEP && c == ',') ) {
        if( cols > 0 && (names[cols-1] = NaN(b,idx,i)).length() > 0 )
          num++;                // Not a number, so take it as a column name
        idx = -1;               // Reset start-of-column
      } else if( c == '"' ) {
        throw new Error("string skipping not implemented");
      } else {                  // Else its just column data
        if( idx == -1 ) {       // Not starting a name?
          cols++;               // Starting a name now
          idx = i;
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
