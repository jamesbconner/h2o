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
}
/*

    private void createValueArrayHeader() {
      assert (_phase == PASS_TWO);
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
*/
