package water.exec;
import java.util.ArrayList;

import water.*;
import water.ValueArray.Column;
import water.ValueArray.ColumnDomain;
import water.parser.ParseDataset;

/** A simple class that automates construction of ValueArrays.
 *
 * @author peta
 */
public class VABuilder {
  private long _numRows;
  private byte _persistence;
  private String _name;
  private ArrayList<Column> _cols = new ArrayList();


  public VABuilder(String name,long numRows) {
    _numRows = numRows;
    _persistence = Value.ICE;
    _name = name;
  }


  public VABuilder addDoubleColumn(String name) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = Double.NaN;
    c._max = Double.NaN;
    c._mean = Double.NaN;
    c._sigma = Double.NaN;
    c._domain = new ColumnDomain();
    _cols.add(c);
    return this;
  }

  public VABuilder addDoubleColumn(String name, double min, double max, double mean) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = Double.NaN;
    c._domain = new ColumnDomain();
    _cols.add(c);
    return this;
  }

  public VABuilder addDoubleColumn(String name, double min, double max, double mean, double sigma) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = -8;
    c._scale = 1;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = sigma;
    c._domain = new ColumnDomain();
    _cols.add(c);
    return this;
  }

  public VABuilder addColumn(String name, int size, int scale, double min, double max, double mean, double sigma) {
    Column c = new Column();
    c._name = name == null ? new String() : name;
    c._size = (byte)size;
    c._scale = (short)scale;
    c._min = min;
    c._max = max;
    c._mean = mean;
    c._sigma = sigma;
    c._domain = new ColumnDomain();
    _cols.add(c);
    return this;
  }

  public VABuilder addColumn(Column other) {
    Column c = new Column();
    c._name = other._name;
    c._size = other._size;
    c._scale = other._scale;
    c._min = other._min;
    c._max = other._max;
    c._mean = other._mean;
    c._sigma = other._sigma;
    c._domain = new ColumnDomain();
    _cols.add(c);
    return this;
  }

  public VABuilder setColumnStats(int colIndex, double min, double max, double mean) {
    Column c = _cols.get(colIndex);
    c._min = min;
    c._max = max;
    c._mean = mean;
    return this;
  }

  public VABuilder setColumnSigma(int colIndex, double sigma) {
    Column c = _cols.get(colIndex);
    c._sigma = sigma;
    return this;
  }

  public ValueArray create(Key k) {
    Column[] cols = _cols.toArray(new Column[_cols.size()]);
    int rowSize = 0;
    for (Column c: cols)
      rowSize += Math.abs(c._size);
    return ValueArray.make(k, _persistence, k, _name, _numRows, rowSize, cols);
  }

  public VABuilder createAndStore(Key k) {
    ValueArray v = create(k);
    TaskPutKey tpk = DKV.put(k,v);
    if( tpk != null ) tpk.get(); // Block for the put
    return this;
  }


  public static ValueArray updateRows(ValueArray old, Key newKey, long newRows) {
    byte[] oldBits = old.get();
    byte[] bits = MemoryManager.allocateMemory(oldBits.length);
    System.arraycopy(oldBits, 0, bits, 0, bits.length);
    UDP.set8(bits,ValueArray.NUM_ROWS_OFF,newRows);
    UDP.set8(bits,ValueArray.LENGTH_OFF,newRows*old.row_size());
    return new ValueArray(newKey,bits);
  }

  public static int chunkSize(Key k, long aryLength, int rowSize) {
    int result = (int) ValueArray.chunk_size();
    result = (result / rowSize) * rowSize; //- (result % rowSize);
    long offset = ValueArray.getChunkIndex(k) * result;
    if (offset + result + result >= aryLength)
      return (int) (aryLength - offset);
    else
      return result;
  }

  static void check(Key k) {
    Value v = DKV.get(k);
    assert (v != null);
    assert (v instanceof ValueArray);
    ValueArray va = (ValueArray) v;
    System.out.println("Num rows:     "+va.num_rows());
    System.out.println("Num cols:     "+va.num_cols());
    System.out.println("Rowsize:      "+va.row_size());
    System.out.println("Length:       "+va.length());
    System.out.println("Rows:         "+((double)va.length() / va.row_size()));
    assert (va.num_rows() == va.length() / va.row_size());
    System.out.println("Chunk size:   "+(ValueArray.chunk_size() / va.row_size()) * va.row_size());
    System.out.println("RPC:          "+ValueArray.chunk_size() / va.row_size());
    System.out.println("Num chunks:   "+va.chunks());
    long totalSize = 0;
    long totalRows = 0;
    for (int i = 0; i < va.chunks(); ++i) {
      System.out.println("  chunk:             "+i);
      System.out.println("    chunk off:         "+ValueArray.chunk_offset(i)+" (reported by VA)");
      System.out.println("    chunk real off:    "+i * ValueArray.chunk_size() / va.row_size() * va.row_size());
      Value c = DKV.get(va.chunk_get(i));
      if (c == null)
        System.out.println("                       CHUNK AS REPORTED BY VA NOT FOUND");
      assert (c!=null):"missing chunk " + i;
      System.out.println("    chunk size:        "+c.length());
      System.out.println("    chunk rows:        "+c.length() / va.row_size());
      byte[] b = c.get();
      assert (b.length == c.length());
      totalSize += c.length();
      System.out.println("    total size:        "+totalSize);
      totalRows += c.length() / va.row_size();
      System.out.println("    total rows:        "+totalRows);
    }
    System.out.println("Length exp:   "+va.length());
    System.out.println("Length:       "+totalSize);
    System.out.println("Rows exp:     "+((double)va.length() / va.row_size()));
    System.out.println("Rows:         "+totalRows);
    assert (totalSize == va.length()):"totalSize: " + totalSize + ", va.length(): " + va.length();
    assert (totalRows == ((double)va.length() / va.row_size()));
  }
}
