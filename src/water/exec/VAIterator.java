
package water.exec;

import java.util.Iterator;
import water.DKV;
import water.Key;
import water.ValueArray;

/**
 *
 * @author peta
 */
public final class VAIterator implements Iterator<VAIterator> {

  public final ValueArray _ary;
  
  public final long _rows;
  
  public final int _rowSize;

  private int _defaultColumn;
  private int _defaultColumnOffset;
  private int _defaultColumnSize;
  private int _defaultColumnBase;
  private int _defaultColumnScale;
  
  private int _rowInChunk;
  private int _rowsInChunk;
  private byte[] _chunkBits;
  private long _chunkOffset;
  private long _currentRow;
  
  public VAIterator(Key k, int defaultColumn, long startRow) {
    _ary = (ValueArray) DKV.get(k);
    assert (_ary != null) : "VA for key "+k.toString()+" not found.";
    _rows = _ary.num_rows();
    _rowSize = _ary.row_size();
    setDefaultColumn(defaultColumn);
    _rowInChunk = -1;
    _rowsInChunk = 0;
    _currentRow = -1;
    if (startRow!=0)
      skipRows(startRow-1);
  }

  public VAIterator(Key key, int defaultColumn) {
    this(key,defaultColumn,0);
  }
  
  public void setDefaultColumn(int colIdx) {
    assert (colIdx>=0) && (colIdx<_ary.num_cols());
    _defaultColumn = colIdx;
    _defaultColumnOffset = _ary.col_off(colIdx);
    _defaultColumnSize = _ary.col_size(colIdx);
    _defaultColumnBase = _ary.col_base(colIdx);
    _defaultColumnScale = _ary.col_scale(colIdx);
  }
  
  public int defaultColumn() {
    return _defaultColumn;
  }
  
  public void skipRows(long rows) {
    assert (_currentRow + rows < _rows);
    while (true) {
      if (_rowInChunk + rows < _rowsInChunk) {
        _rowInChunk += rows;
        break;
      }
      rows -= (_rowsInChunk - _rowInChunk);
      _rowInChunk = _rowsInChunk-1;
      next(); // move to next chunk
    }
  }

  @Override public boolean hasNext() {
    return (_currentRow < _rows);
  }

  @Override public VAIterator next() {
    ++_currentRow;
    ++_rowInChunk;
    if (_rowInChunk == _rowsInChunk) { 
      // load new chunk
      _chunkOffset = _chunkOffset + _rowsInChunk * _rowSize;
      Key k = ValueArray.make_chunkkey(_ary._key, _chunkOffset);
      _chunkBits = DKV.get(k).get();
      _rowsInChunk = _chunkBits.length / _rowSize;
      _rowInChunk = 0;
    }
    return this;
  }
  
  @Override public void remove() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
  public long data() {
    return _ary.data(_chunkBits, _rowInChunk, _rowSize, _defaultColumnOffset, _defaultColumnSize, _defaultColumnBase, _defaultColumnScale, _defaultColumn);
  }
  
  public long data(int column) {
    return _ary.data(_chunkBits,_rowInChunk,_rowSize,column);
  }
  
  public double datad() {
    return _ary.datad(_chunkBits, _rowInChunk, _rowSize, _defaultColumnOffset, _defaultColumnSize, _defaultColumnBase, _defaultColumnScale, _defaultColumn);
  }
  
  public double datad(int column) {
    return _ary.datad(_chunkBits,_rowInChunk,_rowSize,column);
  }
  
  
}
