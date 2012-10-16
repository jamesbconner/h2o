package water.parser;

import java.io.*;
import java.util.*;

import water.*;
import water.parser.ParseDataset.ColumnDomain;
import water.parser.SeparatedValueParser.Row;

public class ParseState {
  public int               _num_cols;       // Input
  public int               _num_rows;       // Output
  public ValueArray.Column _cols[];         // Column summary data
  public int               _rows_chk[];     // Rows-per-chunk
  public ColumnDomain      _cols_domains[]; // Input/Output - columns domains
                                            // preserving insertion order (LinkedSet).

  // Hand-rolled serializer for the above common fields.
  // Some Day Real Soon auto-gen me.
  public int wire_len() {
    assert _num_rows == 0 || _cols != null;
    assert _num_rows == 0 || _rows_chk != null;
    assert _num_rows == 0 || _cols_domains != null;
    int colDomSize = 0;
    if( _cols_domains != null ) for( ColumnDomain cd : _cols_domains )
      colDomSize += cd.wire_len();
    return 4
        + 4
        + 1
        + (_num_rows == 0 ? 0 : (_cols.length * ValueArray.Column.wire_len()
            + 4 + _rows_chk.length * 4)) + colDomSize;
  }

  public void write(Stream s) {
    s.set4(_num_cols);
    s.set4(_num_rows);
    if( _num_rows == 0 ) return; // No columns?
    assert _cols.length == _num_cols;
    for( ValueArray.Column col : _cols )
      col.write(s); // Yes columns; write them all
    // Now the rows-per-chunk array
    s.setAry4(_rows_chk);
    // Write all column domains.
    assert _cols_domains.length == _num_cols;
    for( ColumnDomain coldom : _cols_domains )
      coldom.write(s);
  }

  public void write(DataOutputStream dos) throws IOException {
    dos.writeInt(_num_cols);
    dos.writeInt(_num_rows);
    if( _num_rows == 0 ) return; // No columns?
    assert _cols.length == _num_cols;
    for( ValueArray.Column col : _cols )
      col.write(dos); // Yes columns; write them all
    // Now the rows-per-chunk array
    TCPReceiverThread.writeAry(dos, _rows_chk);
    // Write all column domains.
    assert _cols_domains.length == _num_cols;
    for( ColumnDomain coldom : _cols_domains )
      coldom.write(dos);
  }

  public void read(Stream s) {
    _num_cols = s.get4();
    _num_rows = s.get4();
    if( _num_rows == 0 ) return; // No rows, so no cols
    assert _cols == null;
    _cols = new ValueArray.Column[_num_cols];
    for( int i = 0; i < _num_cols; i++ )
      _cols[i] = ValueArray.Column.read(s);
    _rows_chk = s.getAry4();
    _cols_domains = new ColumnDomain[_num_cols];
    for( int i = 0; i < _num_cols; i++ )
      _cols_domains[i] = ColumnDomain.read(s);
  }

  public void read(DataInputStream dis) throws IOException {
    _num_cols = dis.readInt();
    _num_rows = dis.readInt();
    if( _num_rows == 0 ) return; // No rows, so no cols
    assert _cols == null;
    _cols = new ValueArray.Column[_num_cols];
    for( int i = 0; i < _num_cols; i++ )
      _cols[i] = ValueArray.Column.read(dis);
    _rows_chk = TCPReceiverThread.readIntAry(dis);
    _cols_domains = new ColumnDomain[_num_cols];
    for( int i = 0; i < _num_cols; i++ )
      _cols_domains[i] = ColumnDomain.read(dis);
  }

  public void assignColumnNames(String[] names) {
    assert names == null || names.length == _num_cols;
    for( int i = 0; i < _num_cols; i++ )
      _cols[i]._name = names != null ? names[i] : "";
  }

  public void prepareForStatsGathering() {
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

  public void addRowToStats(Row row) {
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

  public void finishStatsGathering(int idx) {
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

  public void mergeStats(ParseState s) {
    _num_rows += s._num_rows;
    _num_cols = Math.max(_num_cols, s._num_cols);

    if( _cols == null ) {     // No local work?
      _cols = s._cols;
    } else {
      if(_cols.length <= _num_cols){
        ValueArray.Column [] newCols = new ValueArray.Column[_num_cols];
        System.arraycopy(_cols, 0, newCols, 0, _cols.length);
        for(int i = _cols.length; i < _num_cols; ++i){
          newCols[i] = new ValueArray.Column();
        }
        _cols = newCols;
      }
      for( int i=0; i<s._num_cols; i++ ) {
        ValueArray.Column c =    _cols[i];
        ValueArray.Column d = s._cols[i];
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
    int r1[] =   _rows_chk;
    int r2[] = s._rows_chk;
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
    ColumnDomain[] d1 =   _cols_domains;
    ColumnDomain[] d2 = s._cols_domains;
    if( d1 == null ) d1 = d2;
    else {
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
    s._cols         = null;
    s._rows_chk     = null;
    s._cols_domains = null;
  }

  public HashMap<String,Integer>[] createColumnIndexes() {
    HashMap<String,Integer>[] columnIndexes = new HashMap[_num_cols];
    for (int i = 0; i < _num_cols; i++) {
      if (_cols_domains[i].size() == 0) continue;
      columnIndexes[i] = new HashMap<String, Integer>();
      int j = 0;
      for (String s : _cols_domains[i]._domainValues) {
        columnIndexes[i].put(s,j++);
      }
    }
    return columnIndexes;
  }

  public void addRowToBuffer(byte[] buf, int off, double[] sumerr, Row row,
      HashMap<String, Integer>[] columnIndexes) {
    for( int i=0; i< _cols.length; i++ ) {
      double d = row._fieldVals[i];
      ValueArray.Column col = _cols[i];
      if ( columnIndexes[i] != null) {
        assert Double.isNaN(d);
        d = columnIndexes[i].get(row._fieldStringVals[i]).intValue();
      }
      // Write to compressed values
      if( !Double.isNaN(d) ) { // Broken data on row?
        sumerr[i] += (col._mean - d) * (col._mean - d);
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
  }

  // Filter too big column enum domains and setup columns
  // which have enum domain.
  public void filterColumnsDomains() {
    for( int i = 0; i < _num_cols; ++i ) {
      ParseDataset.ColumnDomain colDom   = _cols_domains[i];
      ValueArray.Column col = _cols[i];
      col._domain = colDom;
      // Column domain contains column name => exclude column name from domain.
      if (colDom._domainValues.contains(col._name)) {
        colDom._domainValues.remove(col._name);
      }
      // The column's domain contains too many unique strings => drop the
      // domain. Column is already marked as erroneous.
      if( colDom.size() > ParseDataset.ColumnDomain.DOMAIN_MAX_VALUES) colDom.kill();
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

  public void computeColumnSize() {
    for( ValueArray.Column c : _cols ) {
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

  public int computeOffsets() {
    int row_size = 0;
    int col_off = 0;
    int max_col_size=0;
    for( ValueArray.Column c : _cols ) {
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
    int rs[] = _rows_chk;
    int rs2[] = new int[rs.length+1]; // One larger to hold final number of rows
    int off = 0;
    for( int i=0; i<rs.length; i++ ) {
      rs2[i] = off;
      off += rs[i];
    }
    rs2[rs.length] = off;
    _rows_chk = rs2;
    return row_size;
  }
}