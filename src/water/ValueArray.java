package water;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.parser.ParseDataset.ColumnDomain;

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
  public ValueArray( Key key, long sz, byte mode ) {
    super(COLUMN0_OFF,COLUMN0_OFF,key,mode);
    // The unstructured-data array has zero everywhere, except the length
    UDP.set8(_mem,LENGTH_OFF,sz);
  }

  public ValueArray( int max, int len, Key key, byte mode ) {
    super(max,len,key,mode);
  }

  @Override public long length() { return UDP.get8(get(LENGTH_OFF+8),LENGTH_OFF); }

  @Override public byte type() { return ARRAY; }

  @Override protected boolean getString_impl( int len, StringBuilder sb ) {
    sb.append("[array] size=").append(length());
    return true;
  }

  // Make a system chunk-key from an offset into the main array.
  // No auto-create-on-demand for arraylet sub-parts
  public static Key make_chunkkey( Key key, long off ) {
    assert ((off >> LOG_CHK)<<LOG_CHK) == off;
    // The key layout is:
    //  0 - Key.ARRAYLET_CHUNK
    //  0 - No IP forcing
    //  12345678 - offset
    //  key name
    byte[] kb = new byte[2+8+key._kb.length];
    UDP.set8(kb,2,off);         // Blast down the offset.
    System.arraycopy(key._kb,0,kb,2+8,key._kb.length);
    Key k = Key.make(kb,(byte)key.desired()); // Presto!  A unique arraylet key!
    return k;
  }
  public Key make_chunkkey( long off ) { return make_chunkkey(_key,off); }

  // Get the chunk with the given index.
  // The returned key is *not* guaranteed to exist in K/V store.
  // @param k - key of arraylet chunk
  // @param index - the requested chunk number
  // @return - Key of the chunk with offset == index
  public static Key getChunk(Key k, long index) {
    assert k._kb[0] == Key.ARRAYLET_CHUNK;
    byte[] arr = k._kb.clone();
    long n = ((long) index) << ValueArray.LOG_CHK;
    UDP.set8(arr, 2, n);
    return Key.make(arr);
  }

  // Number of chunks in this array
  // Divide by 1Meg into chunks.  The last chunk is between 1 and 2 megs
  static public long chunks(long sz) { return sz>>LOG_CHK; }
  @Override public long chunks() {
    long num = chunks(length()); // Rounds down: last chunk can be large
    if( num==0 && length() > 0 ) num = 1; // Always at least one, tho
    return num;
  }

  // Get a Key for the chunk; fetching the Value for this Key gets this chunk
  @Override public Key chunk_get( long chunknum ) {
    if( chunknum < 0 || chunknum > chunks() )
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

  // Get a chunk - expected to exist
  Value get( long idx ) throws IOException {
    Value v = DKV.get(chunk_get(idx));
    if( v != null ) return v;
    throw new IOException("Missing chunk "+idx+", broken "+H2O.OPT_ARGS.ice_root+"?");
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

    // try to read 2-chunks into the buffer
    byte[] buffer = new byte[(int)chunk_size()*2];
    int sz = 0;
    while (sz!=buffer.length) {
      int i = bis.read(buffer,sz,buffer.length-sz);
      if (i==-1)
        break;
      sz += i;
    }
    if (sz<buffer.length) { // buffer is 2-chunks
      // it is a single simple value
      Value val = new Value(key,sz);
      System.arraycopy(buffer,0,val._mem,0,sz);
      UKV.put(key,val);
    } else { // sz == buffer.length => there is enough data to write two 1MG chunks
      assert sz == buffer.length;
      long offset = 0;
      Key ck = null; Value val = null;

      while (true) {
        if (sz < buffer.length) { // almost 2 chunks are in the buffer => write them all
          ck = make_chunkkey(key,offset);
          val = new Value(ck,sz);
          System.arraycopy(buffer,0,val._mem,0,sz);
          DKV.put(ck,val);
          offset += sz;
          break; // it was the last chunk => there are no more data in input stream;
        } else { // Write two chunks into the buffer
          byte chunkCtr = 0;
          while (chunkCtr < 2) {
            ck = make_chunkkey(key,offset);
            val = new Value(ck,(int)chunk_size());
            System.arraycopy(buffer,chunkCtr*(int)chunk_size(),val._mem,0,(int)chunk_size());
            DKV.put(ck,val);
            chunkCtr++;
            offset += chunk_size();
          }
        }

        // try to read another two chunks
        sz = 0;
        while (sz!=buffer.length) {
          int i = bis.read(buffer,sz,buffer.length-sz);
          if (i==-1)
            break;
          sz += i;
        }
      }
      ValueArray ary = new ValueArray(key,offset,ICE);
      DKV.put(key,ary);
    }
    bis.close();
    return key;
  }

  // Read a (possibly VERY large file) and put it in the K/V store and return a
  // Value for it.  Files larger than 2Meg are broken into arraylets of 1Meg each.
  static public Key read_put_file(String keyname, FileInputStream fis, byte rf) throws IOException {
    final long sz = fis.getChannel().size();
    final DataInputStream dis = new DataInputStream(fis);
    // Main Key
    final Key key = Key.make(keyname,rf);
    // Files of modest size, from 0 to <2Megs we represent as a single Value.
    // Larger files are broken up in 1Meg chunks
    long chunks = chunks(sz);   // Divide by 1Meg into chunks, rounding up
    if( chunks < 2 ) {          // Not enough chunks, so use a single Value
      Value val = new Value(key,(int)sz);
      dis.readFully(val._mem);
      UKV.put(key,val);         // Insert in distributed store
      return key;
    }

    // Must be a large file; break it up!  Main key first
    ValueArray ary = new ValueArray(key,sz,ICE);
    UKV.put(key,ary);         // Insert in distributed store

    // Begin to read & build chunks.
    long off = 0;
    for( int i=0; i<chunks; i++ ) {
      // All-the-rest for the last chunk, or 1Meg for other chunks
      long szl = (i==chunks-1) ? (sz-off) : (1<<LOG_CHK);
      int sz2 = (int)szl;       // Truncate
      assert sz2 == szl;        // No int/long truncation

      Key ckey = make_chunkkey(key,off);
      Value val = new Value(ckey,sz2);
      dis.readFully(val._mem);

      DKV.put(ckey,val);         // Insert in distributed store

      off += szl;               // Advance the cursor
    }
    assert off == sz;           // Got them all

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
  static private final int NUM_ROWS_OFF=LENGTH_OFF  +8; // Number of rows; length=#rows*size(row)
  static private final int PRIORKEY_OFF=NUM_ROWS_OFF+8; // prior key string offset
  static private final int XFORM_OFF   =PRIORKEY_OFF+4; // prior xforms string offset
  static private final int ROW_SIZE_OFF=XFORM_OFF   +4; // Size of each row (sum of column widths)
  static private final int NUM_COLS_OFF=ROW_SIZE_OFF+4; // number of columns; 0 for unstructured data
  static private final int PAD_OFF     =NUM_COLS_OFF+4; // pad to multiple of 8
  static private final int COLUMN0_OFF =PAD_OFF     +4; // Start of column 0 metadata

  // Most datasets are obtained by transformations on a prior set.
  // The prior set, or null if none
  public Key prior_key() {
    byte[] mem = get();
    int off = UDP.get4(mem,PRIORKEY_OFF);
    if( off==0 ) return null;
    return Key.read(mem,off);
  }
  // The transformation leading to this key, from the prior key
  public String xform() {
    byte[] mem = get();
    int off = UDP.get4(mem,XFORM_OFF);
    if( off==0 ) return null;
    int len = UDP.get2(mem,off);
    return new String(mem,off+2,len);
  }

  // Number of columns in this dataset.  0 for not-structured data.
  public int  num_cols() { return UDP.get4(get(),NUM_COLS_OFF); }
  // Number of rows    in this dataset.  0 for not-structured data.
  public long num_rows() { return UDP.get8(get(),NUM_ROWS_OFF); }
  // Size of each row (sum of column widths) in bytes
  public int  row_size() { return UDP.get4(get(),ROW_SIZE_OFF); }


  // Additional column layout (meta-data); repeat per column
  static private final int    MAX_COL_OFF =0;                // max in column
  static private final int    MIN_COL_OFF =   MAX_COL_OFF+8; // min in column
  static private final int   BASE_COL_OFF =   MIN_COL_OFF+8; // base-offset for all; often 0
  static private final int   NAME_COL_OFF =  BASE_COL_OFF+4; // name offset in the array header
  static private final int    OFF_COL_OFF =  NAME_COL_OFF+4; // offset to column data within row
  static private final int  SCALE_COL_OFF =   OFF_COL_OFF+2; // scale for all; often 1
  static private final int  BADAT_COL_OFF = SCALE_COL_OFF+2; // number of bad rows, capped at 65535
  static private final int   SIZE_COL_OFF = BADAT_COL_OFF+2; // bytesize of column; 1,2,4,8 or -4,-8 for double
  static private final int DOMAIN_COL_OFF =  SIZE_COL_OFF+1; // domain offset in the array header 
  static private final int   PAD0_COL_OFF =DOMAIN_COL_OFF+4; // pad to 8 bytes
  static private final int  META_COL_SIZE =  PAD0_COL_OFF+5;

  // internal convience class for building structured ValueArrays
  static public class Column {
    public String       _name;
    public ColumnDomain _domain;     // Domain of the column - all the strings which represents the column's domain. The order of the strings corresponds to numbering utilized in dataset.
    public double       _min, _max;  // Min/Max per column; requires a 1st pass to discover
    public int          _base;       // Base
    public short        _off;        // Offset of column data within row
    public short        _scale;      // Actual value is (((double)(stored_value+base))/scale); 1,10,100,1000
    public char         _badat;      // Number of bad rows, capped at 65535
    public byte         _size;       // Size is 1,2,4 or 8 bytes, or -4,-8 for float/double data

    public Column() {
      _min =  Double.MAX_VALUE;
      _max = -Double.MAX_VALUE;
      _scale = 1;
      _base = 0;
    }

    static public int wire_len() { return META_COL_SIZE; }

    public int write( byte[] buf, int off ) {
      UDP.set8d(buf,off+  MAX_COL_OFF,_max);
      UDP.set8d(buf,off+  MIN_COL_OFF,_min);
      UDP.set4 (buf,off+ BASE_COL_OFF,_base);
      //                 NAME_COL_OFF is filled in later
      UDP.set2 (buf,off+  OFF_COL_OFF,_off);
      UDP.set2 (buf,off+SCALE_COL_OFF,_scale);
      UDP.set2 (buf,off+BADAT_COL_OFF,_badat);
                buf[off+ SIZE_COL_OFF]=_size;
      return off+META_COL_SIZE;
    }

    public void write( DataOutputStream dos ) throws IOException {
      dos.writeDouble(_max);
      dos.writeDouble(_min);
      dos.writeInt(_base);
      dos.writeShort(_off);
      dos.writeShort(_scale);
      dos.writeShort(_badat);
      dos.writeByte(_size);
    }

    static public Column read( byte[] buf, int off ) {
      Column col = new Column();
      col._max  =       UDP.get8d(buf,off+  MAX_COL_OFF);
      col._min  =       UDP.get8d(buf,off+  MIN_COL_OFF);
      col._base =       UDP.get4 (buf,off+ BASE_COL_OFF);
      col._off  =(short)UDP.get2 (buf,off+  OFF_COL_OFF);
      col._scale=(short)UDP.get2 (buf,off+SCALE_COL_OFF);
      col._badat= (char)UDP.get2 (buf,off+BADAT_COL_OFF);
      col._size =       buf[off+ SIZE_COL_OFF];
      return col;
    }

    static public Column read( DataInputStream dis ) throws IOException {
      Column col = new Column();
      col._max  = dis.readDouble();
      col._min  = dis.readDouble();
      col._base = dis.readInt();
      col._off  = dis.readShort();
      col._scale= dis.readShort();
      col._badat= dis.readChar();
      col._size = dis.readByte();
      return col;
    }
  }

  // Byte offset to column meta-data within the ValueArray
  private final int col(int cnum) {
    if( 0 <= cnum && cnum < num_cols() )
      return cnum*META_COL_SIZE + COLUMN0_OFF;
    throw new ArrayIndexOutOfBoundsException(cnum);
  }

  // Column name (may be null)
  public String col_name(int cnum) {
    byte[] mem = get();
    int off = UDP.get4(mem,col(cnum)+NAME_COL_OFF);
    int len = UDP.get2(mem,off);
    return len > 0 ? new String(mem,off+2,len) : null;
  }
  // All the column names.  Unlike the above version, this one replaces null
  // strings with a column number and never returns null names
  public String[] col_names() {
    final int num_cols = num_cols();
    String[] names = new String[num_cols];
    for( int i=0; i<num_cols; i++ ) {
      String s = col_name(i);
      names[i] = (s==null)?Integer.toString(i):s;
    }
    return names;
  }
  
  // Column domain (may be empty).
  // Index in array corresponds to number in table cell.
  public String[] col_enum_domain(int cnum) {
    byte[] mem = get();
    int off        = UDP.get4(mem,col(cnum)+DOMAIN_COL_OFF);
    int domainSize = UDP.get2(mem, off); off += 2;
    String[] domain = new String[domainSize];
    for( int i = 0; i < domainSize; i++) {
      int len = UDP.get2(mem, off); off += 2;
      domain[i] = len > 0 ? new String(mem, off, len) : null;
      off += len;
    }
    
    return domain;    
  }
  
  // Returns string representation of of given ord in column domain. 
  public String col_enum_domain_val(int cnum, int ord) {
    byte[] mem = get();
    int off        = UDP.get4(mem,col(cnum)+DOMAIN_COL_OFF);
    int domainSize = UDP.get2(mem, off); off += 2;
    if (ord < 0 || ord >= domainSize) throw new ArrayIndexOutOfBoundsException(ord);
    for( int i = 0; i < ord; i++) {
      int len = UDP.get2(mem, off); off += (2+len);      
    }
    int len = UDP.get2(mem, off); off += 2;
    return len > 0 ? new String(mem, off, len) : null;
  }
  
  // Returns true if column's enum domain is not empty
  public boolean col_has_enum_domain(int cnum) {
    byte[] mem = get();
    int off        = UDP.get4(mem,col(cnum)+DOMAIN_COL_OFF);
    int domainSize = UDP.get2(mem, off); off += 2;
    return domainSize > 0;        
  }

  // Offset (within a row) of this column start
  public int col_off(int cnum) { return UDP.get2(get(),col(cnum)+OFF_COL_OFF)&0xFFFF; }

  // Size in bytes of this column, either 1,2,4 or 8 (integer) or -4 or -8 (float/double)
  public int col_size(int cnum) { return get()[col(cnum)+SIZE_COL_OFF]; }

  // Max/min/base/scale value seen in column
  public double col_max  (int cnum) { return UDP.get8d(get(),col(cnum)+  MAX_COL_OFF); }
  public double col_min  (int cnum) { return UDP.get8d(get(),col(cnum)+  MIN_COL_OFF); }
  public int    col_base (int cnum) { return UDP.get4 (get(),col(cnum)+ BASE_COL_OFF); }
  public int    col_scale(int cnum) { return UDP.get2 (get(),col(cnum)+SCALE_COL_OFF); }
  public int    col_badat(int cnum) { return UDP.get2 (get(),col(cnum)+BADAT_COL_OFF)&0xFFFF; }

  // Row# when offset from chunk start
  public final int row_in_chunk(long row, int rpc, long chknum) {
    long rows = chknum*rpc; // Number of rows so far; row-start in this chunk
    return (int)(row - rows);
  }
  public final long chunk_for_row( long row, int rpc ) {
    long chknum = row/rpc;
    if( chknum > 0 && chknum == chunks() ) chknum--; // Last chunk is large
    return chknum;
  }

  // Value extracted, then scaled & based - the double version.  Note that this
  // is not terrible efficient, and that 99% of this code I expect to be loop-
  // invariant when run inside real numeric loops... but that the compiler will
  // probably need help pulling out the loop invariants.
  public double datad(long rownum, int colnum) {
    int rpc = (int)(chunk_size()/row_size()); // Rows per chunk
    long chknum = chunk_for_row(rownum,rpc);
    int row_in_chunk = row_in_chunk(rownum,rpc,chknum);
    Key k = chunk_get(chknum);  // Get the chunk key
    Value val = DKV.get(k);     // Get the chunk
    // Get the whole row.  Note that in structured arrays, no row splits a chunk.
    byte[] bits = val.get((row_in_chunk+1)*row_size());
    return datad(bits,row_in_chunk,row_size(),colnum);
  }

  // This is a version where the colnum data is not yet pulled out.
  public double datad(byte[] bits, int row_in_chunk, int row_size, int colnum) {
    return datad(bits,row_in_chunk,row_size,col_off(colnum),col_size(colnum), col_base(colnum), col_scale(colnum), colnum);
  }

  // This is a version where all the loop-invariants are hoisted already.
  public double datad(byte[] bits, int row_in_chunk, int row_size, int col_off, int col_size, int col_base, int col_scale, int colnum) {
    assert row_size() == row_size;
    assert col_off  (colnum)==col_off  ;
    assert col_base (colnum)==col_base ;
    assert col_scale(colnum)==col_scale;
    assert col_size (colnum)==col_size ;
    int off = (row_in_chunk * row_size) + col_off;
    double res=0;
    switch( col_size ) {
    case  1:         res =    0xff&  bits[off]; break;
    case  2:         res = UDP.get2 (bits,off); break;
    case  4:return         UDP.get4 (bits,off);
    case  8:return         UDP.get8 (bits,off); // No scale/offset for long   data
    case -4:return         UDP.get4f(bits,off); // No scale/offset for float  data
    case -8:return         UDP.get8d(bits,off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    return (res+col_base)/col_scale;
  }

  // Value extracted, then scaled & based - the integer version.
  public long data(long rownum, int colnum) throws IOException {
    int rpc = (int)(chunk_size()/row_size()); // Rows per chunk
    long chknum = chunk_for_row(rownum,rpc);
    int row_in_chunk = row_in_chunk(rownum,rpc,chknum);
    int off = row_in_chunk * row_size();
    // Get the whole row.  Note that in structured arrays, no row splits a chunk.
    byte[] bits = get(chknum).get(off+row_size());
    return data(bits,row_in_chunk,row_size(),colnum);
  }
  public long data(byte[] bits, int row_in_chunk, int row_size, int colnum) {
    return data(bits,row_in_chunk,row_size,col_off(colnum),col_size(colnum), col_base(colnum), col_scale(colnum), colnum);
  }
  // This is a version where all the loop-invariants are hoisted already.
  public long data(byte[] bits, int row_in_chunk, int row_size, int col_off, int col_size, int col_base, int col_scale, int colnum) {
    assert row_size() == row_size;
    assert col_off  (colnum)==col_off  ;
    assert col_base (colnum)==col_base ;
    assert col_scale(colnum)==col_scale;
    assert col_size (colnum)==col_size ;
    int off = (row_in_chunk * row_size) + col_off;
    double res=0;
    switch( col_size ) {
    case  1:       res =    0xff&  bits[off]; break;
    case  2:       res = UDP.get2 (bits,off); break;
    case  4:return       UDP.get4 (bits,off);
    case  8:return       UDP.get8 (bits,off); // No scale/offset for long   data
    case -4:return (long)UDP.get4f(bits,off); // No scale/offset for float  data
    case -8:return (long)UDP.get8d(bits,off); // No scale/offset for double data
    }
    // Apply scale & base for the smaller numbers
    return (long)((res+col_base(colnum))/col_scale(colnum));
  }


  // Test if the value is valid, or was missing in the orginal dataset
  public boolean valid(long rownum, int colnum) throws IOException {
    int rpc = (int)(chunk_size()/row_size()); // Rows per chunk
    long chknum = chunk_for_row(rownum,rpc);
    int row_in_chunk = row_in_chunk(rownum,rpc,chknum);
    // Get the whole row.  Note that in structured arrays, no row splits a chunk.
    byte[] bits = get(chknum).get();
    return valid(bits,row_in_chunk,row_size(),colnum);
  }
  public boolean valid(byte[] bits, int row_in_chunk, int row_size, int colnum ) {
    return valid(bits,row_in_chunk,row_size,col_off(colnum),col_size(colnum));
  }
  // Test if the value is valid, or was missing in the orginal dataset
  // This is a version where all the loop-invariants are hoisted already.
  public boolean valid(byte[] bits, int row_in_chunk, int row_size, int col_off, int col_size ) {
    int off = (row_in_chunk * row_size) + col_off;
    switch( col_size ) {
    case  1:  return           bits[off] != -1;
    case  2:  return UDP.get2 (bits,off) != 65535;
    case  4:  return UDP.get4 (bits,off) != Integer.MIN_VALUE;
    case  8:  return UDP.get8 (bits,off) !=    Long.MIN_VALUE;
    case -4:  return ! Float.isNaN(UDP.get4f(bits,off));
    case -8:  return !Double.isNaN(UDP.get8d(bits,off));
    }
    return false;
  }

  @SuppressWarnings("deprecation")
  static public ValueArray make(Key key, byte persistence_mode, Key priorkey, String xform, long num_rows, int row_size, Column[] cols ) {
    // Size of base meta-data, plus column meta-data.
    int sz = COLUMN0_OFF+cols.length*META_COL_SIZE;
    // Also include String column-name metadata
    for( Column column : cols )
      sz += column._name.length()+2/*2 bytes of pre-length*/;
    // Also priorkey & xform
    sz += priorkey.wire_len()+xform.length()+2;
    // Also include meta-data representing column domains.
    for( Column column : cols)
      sz += column._domain.wire_len();
    // Make it.
    ValueArray ary = new ValueArray(sz,sz,key,persistence_mode);
    byte[] mem = ary._mem;
    // Fill it.
    UDP.set8(mem,LENGTH_OFF,(num_rows*row_size));
    UDP.set4(mem,NUM_COLS_OFF,cols.length);
    UDP.set4(mem,ROW_SIZE_OFF,row_size);
    UDP.set8(mem,NUM_ROWS_OFF,num_rows);
    int i=0;
    for( Column column : cols ) // Fill the columns
      column.write(mem,ary.col(i++));
    // Offset for data past the columns
    int off = cols.length*META_COL_SIZE + COLUMN0_OFF;
    // Prior key
    UDP.set4(mem,PRIORKEY_OFF,off);
    off = priorkey.write(mem,off);
    // XForm string, with leading 2 bytes of length
    UDP.set4(mem,XFORM_OFF,off);
    UDP.set2(mem,off,xform.length()); off += 2;
    xform.getBytes(0,xform.length(),mem,off); off += xform.length();
    // Now the column names
    i=0;
    for( Column column : cols ) {
      String name = column._name;
      UDP.set4(mem,ary.col(i++)+NAME_COL_OFF,off); // First the offset to the name
      UDP.set2(mem,off,name.length()); off += 2;   // Then the name length
      // Then the name bytes itself
      name.getBytes(0,name.length(),mem,off); off += name.length();
    }
    // Now the columns meta-data: domains
    
    i=0;
    for( Column column : cols ) {      
      UDP.set4(mem,ary.col(i++)+DOMAIN_COL_OFF,off);     // First write the offset of domain to column header.
      off = column._domain.write(mem, off);
/*      UDP.set2(mem,off,column._domain.length); off += 2; // Write domain size. 
      for (String s : column._domain) {                  // Write whole domain as pairs: <size of string><byte of string>
        byte[] data = s.getBytes();
        UDP.set2(mem,off,data.length); off += 2;
        System.arraycopy(data, 0, mem, off, data.length); off += data.length;                
      }*/
    }    
        
    return ary;
  }
}
