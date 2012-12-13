
package water.exec;

import water.*;

/** Slice filter!
 *
 * This filter is invoked on DEST, not on source argument!!!
 *
 * @author peta
 */
public class SliceFilter extends MRTask {

  Key _source;
  long _start;
  long _length;
  int _rowSize;
  long _filteredRows;

  public SliceFilter(Key source, long start, long length) {
    _source = source;
    _start = start;
    _length = length;
    ValueArray ary = ValueArray.value(source);
    assert (start + length <= ary.numRows());
    _rowSize = ary._rowsize;
  }


  @Override public void map(Key key) {
    long startRow = ValueArray.getChunkIndex(key) * (ValueArray.CHUNK_SZ / _rowSize);
    int rowsInChunk = VABuilder.chunkSize(key, _length*_rowSize, _rowSize) / _rowSize;
    VAIterator iter = new VAIterator(_source,0,_start+startRow);
    AutoBuffer bits = new AutoBuffer(rowsInChunk*_rowSize);
    for (int offset = 0; offset < bits.remaining(); offset += _rowSize) {
      iter.next();
      iter.copyCurrentRow(bits,offset);
      ++_filteredRows;
    }
    DKV.put(key, new Value(key,bits.bufClose()));
  }

  @Override public void reduce(DRemoteTask drt) {
    SliceFilter other = (SliceFilter) drt;
    _filteredRows += other._filteredRows;
  }

}
