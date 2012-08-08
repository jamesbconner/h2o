package water;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Large Arrays & Arraylets
 *
 * Large arrays are broken into 1Meg chunks (except the last chunk which may be
 * from 1 to 2Megs).  Large arrays have a metadata section in this ValueArray.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 * @version 1.0
 */


public class ValueArray extends Value {

  public static final int LOG_CHK = 20; // Chunks are 1<<20, or 1Meg

  // Large datasets need metadata.   :-)
  // Which is described further below, for structured ValueArrays.

  // The unstructured-data array
  public ValueArray( Key key, long sz ) {
    super(key,NUM_COLS_OFF+2);
    // The unstructured-data array has zero everywhere, except the length
    UDP.set8(_mem,LENGTH_OFF,sz);
  }
  
  public ValueArray( int max, int len, Key key, int mode ) {
    super(max,len,key,mode);
  }
  
  @Override public long length() { return UDP.get8(get(),LENGTH_OFF); }

  @Override public byte type() { return ARRAYLET; }

  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[array] size=").append(length());
    return true;
  }

  // Make a system chunk-key from an offset into the main array.
  // Lazily manifest data chunks on demand.
  public Key make_chunkkey( long off ) {
    assert ((off >> LOG_CHK)<<LOG_CHK) == off;
    // The key layout is:
    //  0 - Key.ARRAYLET_CHUNK
    //  0 - No IP forcing
    //  12345678 - offset
    //  key name
    byte[] kb = new byte[2+8+_key._kb.length];
    UDP.set8(kb,2,off);         // Blast down the offset.
    System.arraycopy(_key._kb,0,kb,2+8,_key._kb.length);
    Key k = Key.make(kb,(byte)_key.desired()); // Presto!  A unique arraylet key!
    return k;
  }

  // Get the chunk with the given index.
  // The returned key is *not* guaranteed to exist in K/V store.
  // @param k - key of arraylet chunk
  // @param index - offset of the requested chunk
  // @return - Key of the chunk with offset == index
  public static Key getChunk(Key k, int index) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    byte[] arr = k._kb.clone();
    long n = ((long) index) << ValueArray.LOG_CHK;
    UDP.set8(arr, 2, n);
    return Key.make(arr);
  }

  // Number of chunks in this array
  // Divide by 1Meg into chunks.  The last chunk is between 1 and 2 megs
  static private long chunks(long sz) { return sz>>LOG_CHK; }
  @Override public long chunks() { return chunks(length()); }

  // Convert a chunk-number into a long offset
  @Override public long chunk_offset( long chunknum ) { return chunknum << LOG_CHK; }
  // Get a Key for the chunk; fetching the Value for this Key gets this chunk
  @Override public Key chunk_get( long chunknum ) {
    if( chunknum < 0 || chunknum >= chunks() )
      throw new ArrayIndexOutOfBoundsException(Long.toString(chunknum));
    return make_chunkkey(chunk_offset(chunknum));
  }

  public static long chunk_size() {
    return 1L << LOG_CHK;
  }
  

  // Get the offset from a random arraylet sub-key
  public static long getOffset(Key k) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    return UDP.get8(k._kb, 2);
  }
  // Get the chunk-index from a random arraylet sub-key
  public static int getChunkIndex(Key k) {
    return (int) (getOffset(k) >> ValueArray.LOG_CHK);
  }
  // Get the root array Key from a random arraylet sub-key
  public static byte[] getArrayKeyBytes( Key k ) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    return Arrays.copyOfRange(k._kb,2+8,k._kb.length);
  }

  static public Key read_put_stream(String keyname, InputStream is, byte rf) throws IOException {
    // Main Key
    Key key = Key.make(keyname,rf);
    return read_put_stream(key,is);
  }
  
  // Reads the given stream and creates a value for it, or a list of chunks and
  // an arraylet if the value is too big. 
  // Maybe this should be somehow merged with read_put_file ? 
  static public Key read_put_stream(Key key, InputStream is) throws IOException {
    BufferedInputStream bis = new BufferedInputStream(is,(int)chunk_size()*2);

    byte[] buffer = new byte[(int)chunk_size()*2];
    int sz = 0;
    while (sz!=buffer.length) {
      int i = bis.read(buffer,sz,buffer.length-sz);
      if (i==-1)
        break;
      sz += i;
    }
    if (sz<buffer.length) {
      // it is a single simple value
      Value val = new Value(key,(int)sz);
      System.arraycopy(buffer,0,val._mem,0,(int)sz);
      UKV.put(key,val);
    } else {
      long offset = 0;
      ValueArray ary = new ValueArray(key,0);
      Key ck = ary.make_chunkkey(offset);
      Value val = new Value(ck,(int)chunk_size());
      System.arraycopy(buffer,0,val._mem,0,(int)chunk_size());
      DKV.put(ck,val);
      ck = ary.make_chunkkey(offset);
      val = new Value(ck,(int)chunk_size());
      System.arraycopy(buffer,(int)chunk_size(),val._mem,0,(int)sz);
      DKV.put(ck,val);
      offset += sz;
      while (sz!=0) {
        sz = 0;
        while (sz!=chunk_size()) {
          int i = bis.read(buffer,sz,(int)chunk_size()-sz);
          if (i==-1)
            break;
          sz += i;
        }
        sz = bis.read(buffer,0,(int)chunk_size());
        ck = ary.make_chunkkey(offset);
        val = new Value(ck,(int)sz);
        System.arraycopy(buffer,0,val._mem,0,(int)sz);
        DKV.put(ck,val);
        offset += sz;
        if (sz!=chunk_size())
          break;
      }
      UDP.set8(ary._mem,LENGTH_OFF,offset);
      DKV.put(key,ary);
    }
    bis.close();
    return key;
  }  

  // Read a (possibly VERY large file) and put it in the K/V store and return a
  // Value for it.  Files larger than 2Meg are broken into arraylets of 1Meg each.
  static public Key read_put_file(String keyname, FileInputStream fis, byte rf) throws IOException {
    long sz = fis.getChannel().size();
    DataInputStream dis = new DataInputStream(fis);
    // Main Key
    Key key = Key.make(keyname,rf);
    // Files of modest size, from 0 to <2Megs we represent as a single Value.
    // Larger files are broken up in 1Meg chunks
    long chunks = chunks(sz);   // Divide by 1Meg into chunks, rounding up
    if( chunks < 2 ) {          // Not enough chunks, so use a single Value
      Value val = new Value(key,(int)sz);      
      dis.readFully(val._mem);
      UKV.put(key,val);         // Insert in distributed store
      return key;
    }

    // Must be a large file; break it up!
    ValueArray ary = new ValueArray(key,sz);

    // Begin to read & build chunks.
    long off = 0;
    for( int i=0; i<chunks; i++ ) {
      // All-the-rest for the last chunk, or 1Meg for other chunks
      long szl = (i==chunks-1) ? (sz-off) : (1<<LOG_CHK);
      int sz2 = (int)szl;       // Truncate
      assert sz2 == szl;        // No int/long truncation

      Key ckey = ary.make_chunkkey(off);
      Value val = new Value(ckey,sz2);
      dis.readFully(val._mem);
      
      DKV.put(ckey,val);         // Insert in distributed store

      off += szl;               // Advance the cursor
    }
    assert off == sz;           // Got them all

    // Now insert the main Key
    UKV.put(key,ary);         // Insert in distributed store    
    return key;
  }

  // --------------------------------------------------------
  // Large datasets need metadata.   :-)
  //
  // We describe datasets as either being "raw" ascii or unformatted binary
  // data, or as being "structured binary" data - i.e., binary, floats, or
  // ints.  Structured data is efficient for doing math & matrix manipulation.
  //
  // Structured data is limited to being 2-D, a collection of rows and columns.
  // The count of columns is expected to be small - from 1 to 1000.  The count
  // of rows is unlimited and could be more than 2^32.  We expect data in
  // columns to be highly compressable within a column, as data with a dynamic
  // range of less than 1 byte is common (or equivalently, floating point data
  // with only 2 or 3 digits of accuracy).  Because data volumes matter (when
  // you have billions of rows!), we want to compress the data while leaving it
  // in an efficient-to-use format.
  //
  // The primary compression is to use 1-byte, 2-byte, or 4-byte columns, with
  // an optional offset & scale factor.  These are described in the meta-data.

  // Layout of structured ValueArrays
  static private final int LENGTH_OFF  =0;              // Total byte length
  static private final int NUM_COLS_OFF=LENGTH_OFF  +8; // number of columns; 0 for unstructured data
  static private final int PRIORKEY_OFF=NUM_COLS_OFF+2; // prior key string offset
  static private final int XFORM_OFF   =PRIORKEY_OFF+2; // prior xforms string offset
  static private final int ROW_SIZE_OFF=XFORM_OFF   +2; // Size of each row (sum of column widths)
  static private final int NUM_ROWS_OFF=ROW_SIZE_OFF+2; // Number of rows; length=#rows*size(row)
  static private final int COLUMN0_OFF =NUM_ROWS_OFF+8; // Start of column 0 metadata

  // Most datasets are obtained by transformations on a prior set.
  // The prior set, or null if none
  Key prior_key() {
    if( UDP.get2(get(),PRIORKEY_OFF)==0 ) return null;
    throw new Error("unimplemented");
  }
  // The transformation leading to this key, from the prior key
  String xform() {
    if( UDP.get2(get(),XFORM_OFF)==0 ) return null;
    throw new Error("unimplemented");
  }

  // Number of columns in this dataset.  0 for not-structured data.
  public int  num_cols() { return UDP.get2(get(),NUM_COLS_OFF); }
  // Number of rows    in this dataset.  0 for not-structured data.
  public long num_rows() { return UDP.get8(get(),NUM_ROWS_OFF); }
  // Size of each row (sum of column widths) in bytes
  public int  row_size() { return UDP.get2(get(),ROW_SIZE_OFF); }

  // Byte-start of a row
  long row_off(long row) {
    if( 0 <= row && row < num_rows() )
      return row_size()*row;
    throw new ArrayIndexOutOfBoundsException(Long.toString(row));
  }

  // Additional column layout; repeat per column
  static private final int  BASE_COL_OFF = 0;              // base-offset for all; often 0
  static private final int SCALE_COL_OFF = BASE_COL_OFF+8; // scale for all; often 1
  static private final int   MAX_COL_OFF =SCALE_COL_OFF+8; // max in column
  static private final int   MIN_COL_OFF =  MAX_COL_OFF+8; // min in column
  static private final int   OFF_COL_OFF =  MIN_COL_OFF+8; // offset to column data within row
  static private final int  NAME_COL_OFF =  OFF_COL_OFF+2; // name offset in the array header
  static private final int  SIZE_COL_OFF = NAME_COL_OFF+2; // bytesize of column; 1,2,4,8 or -4,-8 for double
  static private final int  PAD0_COL_OFF = SIZE_COL_OFF+1;
  static private final int META_COL_SIZE = PAD0_COL_OFF+3;

  // internal convience class for building structured ValueArrays
  static public class Column {
    public String _name;
    public double _base, _scale; // Actual value is (stored_value*scale+base)
    public double _min, _max; // Min/Max per column; requires a 1st pass to discover
    public int _off;          // Offset of column data within row
    public byte _size; // Size is 1,2,4 or 8 bytes, or -4,-8 for float/double data
    
    public Column() {
      _base = 0.0;
      _scale = 1.0;
      _min = Double.MAX_VALUE;
      _max = Double.MIN_VALUE;
    }

    static public int wire_len() { return META_COL_SIZE; }

    public int write( byte[] buf, int off ) {
      UDP.set8(buf,off+ BASE_COL_OFF,_base);
      UDP.set8(buf,off+SCALE_COL_OFF,_scale);
      UDP.set8(buf,off+  MAX_COL_OFF,_max);
      UDP.set8(buf,off+  MIN_COL_OFF,_min);
      UDP.set2(buf,off+  OFF_COL_OFF,_off);
      //                NAME_COL_OFF is filled in later
               buf[off+ SIZE_COL_OFF]=_size;
      //                PAD0_COL_OFF is never filled in
      return off+META_COL_SIZE;
    }

    static public Column read( byte[] buf, int off ) {
      Column col = new Column();
      col._base  = UDP.get8(buf,off+ BASE_COL_OFF);
      col._scale = UDP.get8(buf,off+SCALE_COL_OFF);
      col._max   = UDP.get8(buf,off+  MAX_COL_OFF);
      col._min   = UDP.get8(buf,off+  MIN_COL_OFF);
      col._off   = UDP.get2(buf,off+  OFF_COL_OFF);
      col._size  =          buf[off+ SIZE_COL_OFF];
      return col;
    }
  }

  // Byte offset to column meta-data within the ValueArray
  private final int col(int cnum) {
    if( 0 <= cnum && cnum < num_cols() )
      return cnum*META_COL_SIZE + COLUMN0_OFF;
    throw new ArrayIndexOutOfBoundsException(cnum);
  }

  // Column name (may be the empty string, but not null)
  String col_name(int cnum) {
    int off = UDP.get2(_mem,col(cnum)+NAME_COL_OFF);
    return new String(_mem,off+2,UDP.get2(_mem,off));
  }

  // Offset (within a row) of this column start
  int col_off(int cnum) { return UDP.get2(_mem,col(cnum)+OFF_COL_OFF); }

  // Size in bytes of this column, either 1,2,4 or 8 (integer) or -4 or -8 (float/double)
  int col_size(int cnum) { return _mem[col(cnum)+SIZE_COL_OFF]; }

  // Max/min/base/scale value seen in column
  double col_max  (int cnum) { return UDP.get8d(_mem,col(cnum)+  MAX_COL_OFF); }
  double col_min  (int cnum) { return UDP.get8d(_mem,col(cnum)+  MIN_COL_OFF); }
  double col_base (int cnum) { return UDP.get8d(_mem,col(cnum)+ BASE_COL_OFF); }
  double col_scale(int cnum) { return UDP.get8d(_mem,col(cnum)+SCALE_COL_OFF); }

  // Value extracted, then scaled & based - the double version.  Note that this
  // is not terrible efficient, and that 99% of this code I expect to be loop-
  // invariant when run inside real numeric loops... but that the compiler will
  // probably need help pulling out the loop invariants.
  double datad(long rownum, int colnum) {
    long row_off = row_off(rownum); // Absolute byte of row start
    long chknum = chunks(row_off);  // Chunk number for row start
    int row_start = (int)(row_off - chunk_offset(chknum)); // Offset into the chunk to row start
    Key k = chunk_get(chknum);  // Get the chunk key
    Value val = DKV.get(k);     // Get the chunk
    // Get the whole row.  Note that in structured arrays, no row splits a chunk.
    byte[] bits = val.get(row_start+row_size());
    int col_off = row_start+col_off(colnum);
    double res=0;
    switch( col_size(colnum) ) {
    case  1:         res =           bits[col_off]; break;
    case  2:         res = UDP.get2 (bits,col_off); break;
    case  4:         res = UDP.get4 (bits,col_off); break;
    case  8:return (double)UDP.get8 (bits,col_off); // No scale/offset for long   data
    case -4:return (double)UDP.get4f(bits,col_off); // No scale/offset for float  data
    case -8:return         UDP.get8d(bits,col_off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    return res*col_scale(colnum)+col_base(colnum);
  }

  // Value extracted, then scaled & based - the integer version.
  long data(long rownum, int colnum) {
    long row_off = row_off(rownum); // Absolute byte of row start
    long chknum = chunks(row_off);  // Chunk number for row start
    int row_start = (int)(row_off - chunk_offset(chknum)); // Offset into the chunk to row start
    Key k = chunk_get(chknum);  // Get the chunk key
    Value val = DKV.get(k);     // Get the chunk
    // Get the whole row.  Note that in structured arrays, no row splits a chunk.
    byte[] bits = val.get(row_start+row_size());
    int col_off = row_start+col_off(colnum);
    long res=0;
    switch( col_size(colnum) ) {
    case  1:       res =           bits[col_off]; break;
    case  2:       res = UDP.get2 (bits,col_off); break;
    case  4:       res = UDP.get4 (bits,col_off); break;
    case  8:return       UDP.get8 (bits,col_off); // No scale/offset for long   data
    case -4:return (long)UDP.get4f(bits,col_off); // No scale/offset for float  data
    case -8:return (long)UDP.get8d(bits,col_off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    if( col_scale(colnum)== 1.0 &&
        col_base (colnum)== 0.0 )
      return res;
    return (long)(res*col_scale(colnum)+col_base(colnum));
  }

  final ValueArray make(Key key, byte persistence_mode, Key priorkey, String xform, int num_rows, Column[] cols ) {
    // Size of base meta-data, plus column meta-data.
    int sz = COLUMN0_OFF+cols.length*META_COL_SIZE;
    // Also include String column-name metadata
    for( Column column : cols )
      sz += column._name.length()+2/*2 bytes of pre-length*/;
    // Also priorkey & xform
    sz += priorkey.wire_len()+xform.length()+2;
    // Make it.
    ValueArray ary = new ValueArray(key,sz);
    byte[] mem = ary._mem;
    // Fill it.
    UDP.set2(mem,NUM_COLS_OFF,cols.length);
    UDP.set2(mem,NUM_ROWS_OFF,num_rows);
    int i=0;
    for( Column column : cols ) // Fill the columns
      column.write(mem,col(i++));
    // Offset for data past the columns
    int off = cols.length*META_COL_SIZE + COLUMN0_OFF;
    // Prior key
    UDP.set2(mem,PRIORKEY_OFF,off);
    off = priorkey.write(mem,off);
    // XForm string, with leading 2 bytes of length
    UDP.set2(mem,XFORM_OFF,off);
    UDP.set2(mem,off,xform.length()); off += 2;
    xform.getBytes(0,xform.length(),mem,off); off += xform.length();
    // Now the column names
    i=0;
    for( Column column : cols ) {
      String name = column._name;
      UDP.set2(mem,col(i++),off); // First the offset to the name
      UDP.set2(mem,off,name.length()); off += 2; // Then the name length
      // Then the name bytes itself
      name.getBytes(0,name.length(),mem,off); off += name.length();
    }

    return ary;
  }

}
