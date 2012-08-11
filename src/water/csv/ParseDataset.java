package water.csv;
import java.io.*;
import java.util.Arrays;
import water.*;

// Helper class to parse an entire ValueArray data, and produce a structured
// ValueArray result.
//
// @author <a href="mailto:cliffc@0xdata.com"></a>

public final class ParseDataset {

  // Parse the dataset as a CSV-style thingy and produce a structured dataset
  // result.  This does a distributed parallel parse.
  public static void parse( Key result, Value dataset ) {
    if( dataset instanceof ValueArray && ((ValueArray)dataset).num_cols() > 0 )
      throw new IllegalArgumentException("This is a binary structured dataset; parse() only works on text files.");

    // Guess on the number of columns, build a column array.
    int num_cols = guess_num_cols(dataset);
    //System.out.println("Found "+num_cols1+" columns");
    //int num_cols = CSVParserKV.getNColumns(dataset._key);

    DParse1 dp1 = new DParse1();
    dp1._num_cols = num_cols;
    dp1._num_rows = 0;          // No rows yet

    long start = System.currentTimeMillis();

    dp1.rexec(dataset._key);     // Parse whole dataset!

    long now = System.currentTimeMillis();

    // Now figure out how best to represent the data.
    compute_column_size(dp1);

    // Compute row size & column offsets
    int row_size = 0;
    int col_off = 0;
    int max_col_size=0;
    for( int i=0; i<num_cols; i++ ) {
      ValueArray.Column c= dp1._cols[i];
      c._name = "";             // No column names for now
      int sz = Math.abs(c._size);
      // Dumb-ass in-order columns.  Later we should sort bigger columns first
      // to preserve 4 & 8-byte alignment for larger values
      c._off = (short)col_off; 
      col_off += sz;
      row_size += sz;
      if( sz > max_col_size ) max_col_size = sz;
      //System.out.println("col "+i+" min="+c._min+" max="+c._max+" size="+c._size+" base="+c._base+" scale="+c._scale);
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

    // Setup for pass-2, where we do the actual data conversion.
    DParse2 dp2 = new DParse2();
    dp2._num_cols = num_cols;
    dp2._num_rows = row_size; // Cheat: pass in rowsize instead of the num_rows (and its non-zero)
    dp2._cols     = dp1._cols;
    dp2._rows_chk = rs2;        // Rolled-up row numbers
    dp2._result   = result;

    long start2 = System.currentTimeMillis();

    dp2.rexec(dataset._key);     // Parse whole dataset!

    long now2 = System.currentTimeMillis();

    // Now make the structured ValueArray & insert the main key
    ValueArray ary = ValueArray.make(result, PersistIce.INIT, dataset._key, "basic_parse", dp1._num_rows, row_size, dp1._cols);
    UKV.put(result,ary);
  }

  // ----
  private static void compute_column_size( DParse1 dp1) {
    int num_cols = dp1._cols.length;

    for( int i=0; i<num_cols; i++ ) {
      ValueArray.Column c= dp1._cols[i];
      if( c._size==31 ) {       // All fails; this is a plain double
        c._size = -8;           // Flag as a plain double
        continue;
      } 
      if( c._size == 15){     // All the int-versions fail, but fits in a float
        c._size = -4;           // Flag as a float
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

      // See if we fit in an unbiased byte
      if( 0 <= min && max <= 255 ) { c._size = 1; continue; }
      // See if we fit in a *biased* byte
      if( span <= 255 ) { c._size = 1; c._base = (int)min; continue; }

      // See if we fit in an unbiased short
      if( 0 <= min && max <= 65535 ) { c._size = 2; continue; }
      // See if we fit in a *biased* short
      if( span <= 65535 ) { c._size = 2; c._base = (int)min; continue; }
      // Must be an int, no bias needed.
      c._size = (byte)((c._scale == 1) ? 4 : -4); // Either int or float
    }
  }


  // ----
  // Distributed parsing
  public static abstract class DParse extends DRemoteTask {
    int _num_cols;              // Input
    int _num_rows;              // Output
    ValueArray.Column _cols[];  // Column summary data
    int _rows_chk[];            // Rows-per-chunk
    public int wire_len() {
      assert _num_rows==0 || _cols != null;
      assert _num_rows==0 || _rows_chk != null;
      return 4+4+(_num_rows==0?0:(_cols.length*ValueArray.Column.wire_len() + 4+_rows_chk.length*4));
    }

    public int write( byte[] buf, int off ) { 
      UDP.set4(buf,(off+=4)-4,_num_cols);
      UDP.set4(buf,(off+=4)-4,_num_rows);
      if( _num_rows == 0 ) return off; // No columns?
      assert _cols.length == _num_cols;
      for( ValueArray.Column col : _cols )
        off = col.write(buf,off); // Yes columns; write them all
      // Now the rows-per-chunk array
      UDP.set4(buf,(off+=4)-4,_rows_chk.length);
      for( int x : _rows_chk )
        UDP.set4(buf,(off+=4)-4,x);
      return off;
    }
    public void write( DataOutputStream dos ) { 
      throw new Error("unimplemented"); 
    }
    public void read( byte[] buf, int off ) { 
      _num_cols = UDP.get4(buf,off);  off += 4; 
      _num_rows = UDP.get4(buf,off);  off += 4; 
      if( _num_rows == 0 ) return; // No rows, so no cols
      assert _cols == null;
      _cols = new ValueArray.Column[_num_cols];
      final int l = ValueArray.Column.wire_len();
      for( int i=0; i<_num_cols; i++ )
        _cols[i] = ValueArray.Column.read(buf,(off+=l)-l);
      int rlen = UDP.get4(buf,off);  off += 4;
      _rows_chk = new int[rlen];
      for( int i=0; i<rlen; i++ )
        _rows_chk[i] = UDP.get4(buf,(off+=4)-4);
    }
    public void read( DataInputStream dis ) { 
      new Error("unimplemented"); 
    }
  }

  // ----
  // Distributed parsing, Pass 1
  // Find min/max, digits per column.  Find number of rows per chunk.
  public static class DParse1 extends DParse {

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
      CSVParserKV<double[]> csv = new CSVParserKV<double[]>(key,1,data,null);
      int num_rows = 0;
      for( double[] ds : csv ) {
        num_rows++;
        for( int i=0; i<_num_cols; i++ ) {
          double d = ds[i];
          if( Double.isNaN(d) ) continue;
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
      _num_rows = num_rows;
      // Also pass along the rows-per-chunk
      int idx = key.user_allowed() ? 0 : ValueArray.getChunkIndex(key);
      _rows_chk = new int[idx+1];
      _rows_chk[idx] = num_rows;
    }

    // Combine results
    public void reduce( RemoteTask rt ) {
      DParse1 dp = (DParse1)rt;
      _num_rows += dp._num_rows;
      if( _cols == null ) {     // No local work?
        _cols = dp._cols;
      } else {
        for( int i=0; i<_num_cols; i++ ) {
          ValueArray.Column c =    _cols[i];
          ValueArray.Column d = dp._cols[i];
          if( d._min < c._min ) c._min = d._min; // min of mins
          if( d._max > c._max ) c._max = d._max; // max of maxes
          c._size |=  d._size;                   // accumulate size fail bits
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
  public static class DParse2 extends DParse {
    Key _result;                // The result Key

    // Pass along the result Key
    public int wire_len() {
      return super.wire_len()+_result.wire_len();
    }
    public int write( byte[] buf, int off ) { 
      off = super.write(buf,off);
      off += _result.write(buf,off);
      return off;
    }
    public void read( byte[] buf, int off ) { 
      super.read(buf,off);
      off += super.wire_len();
      _result = Key.read(buf,off);
      assert _result != null;
    }

    // Parse just this chunk, compress into new format.
    public void map( Key key ) {
      assert _cols != null;
      assert _num_rows != 0;
      int row_size = _num_rows; // Cheat: row_size instead of num_rows
      // Get chunk index
      int cidx = key.user_allowed() ? 0 : ValueArray.getChunkIndex(key);
      // Get the starting row of the source chunk
      long start_row = _rows_chk[cidx];
      // Get number of rows in this source chunk
      int num_rows = (int)(_rows_chk[cidx+1]-start_row);
      // Get a place to hold the data
      byte[] buf = new byte[num_rows*row_size];
      // A place to hold each column datum
      double[] data = new double[_cols.length];
      // The parser
      CSVParserKV<double[]> csv = new CSVParserKV<double[]>(key,1,data,null);
      // Fill the rows
      int off = 0;
      for( double[] ds : csv ) {
        int old = off;
        for( int i=0; i<ds.length; i++ ) {
          double d = ds[i];
          ValueArray.Column col = _cols[i];
          switch( col._size ) {
          case  1: buf[off++] = (byte)(d*col._scale-col._base); break;
          case  2: UDP.set2 (buf,(off+=2)-2, (int)(d*col._scale-col._base)); break;
          case  4: UDP.set4 (buf,(off+=4)-4, ( int)d); break;
          case  8: UDP.set8 (buf,(off+=8)-8, (long)d); break;
          case -4: UDP.set4f(buf,(off+=4)-4,(float)d); break;
          case -8: UDP.set8d(buf,(off+=8)-8,       d); break;
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

    public void reduce( RemoteTask rt ) {
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
      long row1 = start_row+row0; // First row to write to
      long chk1 = row1/rpc;       // First chunk to write to
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
      AtomicUnion au = new AtomicUnion();
      au._buf = buf;
      au._src_off = src_off;
      au._dst_off = dst_off;
      au._len = len;
      au.run(key1);             // Start atomic update
      au.complete();            // No need to complete now?
      return rowz;              // Rows written out
    }

    public static class AtomicUnion extends Atomic {
      byte[] _buf;
      int _src_off;
      int _dst_off;
      int _len;
      protected int wire_len() { return 4+4+_len; }
      protected int  write( byte[] buf, int off ) {
        off += UDP.set4(buf,off,_dst_off);
        off += UDP.set4(buf,off,_len);
        System.arraycopy(_buf,_src_off,buf,off,_len);
        return off+_len;
      }
      protected void write( DataOutputStream dos ) throws IOException { 
        dos.writeInt(_dst_off);
        dos.writeInt(_len);
        dos.write(_buf,_src_off,_len);
      }
      protected void read( byte[] buf, int off ) {
        _src_off = 0;
        _dst_off = UDP.get4(buf,(off+=4)-4);
        _len     = UDP.get4(buf,(off+=4)-4);
        _buf = new byte[_len];
        System.arraycopy(buf,off,_buf,0,_len);
      }
      protected void read( DataInputStream dis ) throws IOException { 
        _src_off = 0;
        _dst_off = dis.readInt();
        _len = dis.readInt();
        _buf = new byte[_len];
        dis.readFully(_buf);
      }
      @Override public byte[] atomic( byte[] bits1 ) {
        byte[] bits2 = (bits1 == null)
          ?  new byte[_dst_off+_len] // Initial array of correct size
          : Arrays.copyOf(bits1,Math.max(_dst_off+_len,bits1.length));
        System.arraycopy(_buf,_src_off,bits2,_dst_off,_len);
        return bits2;
      }
    }
  }

  // ---
  // Alternative column guesser
  private static int guess_num_cols( Value dataset ) {
    // Best-guess on count of columns.  Skip 1st line.  Count column delimiters
    // in the next line.
    Value v0 = DKV.get(dataset.chunk_get(0)); // First chunk
    byte[] b = v0.get();                      // Bytes for 1st chunk
    int i=0;
    while( i<b.length && b[i] != '\r' && b[i] != '\n' ) i++;   // Skip a line
    if( i==b.length ) return 0;  // No columns?
    // start counting columns on the 2nd line
    int cols = 0;
    int mode = 0;
    while( true ) {
      char c = (char)b[i++];
      if( c=='\n' || c== '\r' ) {
        break;
      } if( Character.isWhitespace(c) ) {
        if( mode == 1 ) mode = 2;
      } else if( c == ',' ) {
        if( mode == 0 ) cols++;
        mode = 0;
      } else if( c == '"' ) {
        throw new Error("string skipping not implemented");
      } else {
        if( mode != 1 ) cols++;
        mode = 1;
      }
    }
    return cols;
  }
}
