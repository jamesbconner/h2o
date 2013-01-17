package water.exec;

import water.*;

/**
 *
 * @author peta
 */
public abstract class CustomFilter extends MRTask {

  long _filteredRows;

  Key _destKey;

  int _rowSize;


  public CustomFilter(Key destKey) {
    _destKey = destKey;
  }

  protected CustomFilter() { }

  @Override public void map(Key key) {
    ValueArray ary = ValueArray.value(ValueArray.getArrayKey(key));
    AutoBuffer bits = ary.getChunk(key);
    _rowSize = ary._rowsize;
    int len = bits.remaining();
    filterInitMap(ary, key, len);
    AutoBuffer newBits = new AutoBuffer(len);
    for (int offset = 0; offset < len; offset += _rowSize) {
      if (filter(bits,offset)) {
        newBits.copyArrayFrom(0,bits, offset, _rowSize);
        ++_filteredRows;
      }
    }
    Key d = ValueArray.getChunkKey(ValueArray.getChunkIndex(key), _destKey);
    Value v = new Value(d, newBits.buf());
    DKV.put(d,v);
  }

  @Override public void reduce(DRemoteTask drt) {
    CustomFilter other = (CustomFilter) drt;
    _filteredRows += other._filteredRows;
  }


  /** This is the filter function. It is given a byte array, that is the
   * currently worked on input chunk and the rowOffset. Should return true if
   * the row should be included in the output, or false if the row is not to
   * be included in the output.
   *
   * @param bits
   * @param rowOffset
   * @return
   */
  protected abstract boolean filter(AutoBuffer bits, int rowOffset);

  /** Override this if you need some code to be called when map is
   *
   * @param rows
   */
  protected void filterInitMap(ValueArray ary, Key k, int rows) {
    // pass
  }
}

// =============================================================================
// BooleanVectorFilter
// =============================================================================

class BooleanVectorFilter extends CustomFilter {
  Key _bVect;
  int _bCol;
  transient VAIterator _bIter;

  public BooleanVectorFilter(Key destKey, Key bVect, int bCol) {
    super(destKey);
    _bVect = bVect;
    _bCol = bCol;
  }

  @Override protected boolean filter(AutoBuffer bits, int rowOffset) {
    _bIter.next();
    return _bIter.datad() != 0;
  }

  @Override protected void filterInitMap(ValueArray ary, Key k, int bytes) {
    long row = ary.startRow(ValueArray.getChunkIndex(k));
    _bIter = new VAIterator(_bVect,_bCol,row);
  }
}



