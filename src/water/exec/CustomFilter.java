
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
    int wo = 0;
    _rowSize = ary._rowsize;
    filterInitMap(ary, key, bits.remaining());
    AutoBuffer newBits = new AutoBuffer(bits.remaining());
    for (int offset = 0; offset < bits.remaining(); offset += _rowSize) {
      if (filter(bits,offset)) {
        ++_filteredRows;
        newBits.copyArrayFrom(wo,bits, offset, _rowSize);
//        System.arraycopy(bits._bb.array(),offset,newBits._bb.array(),wo,_rowSize);
        wo += _rowSize;
      }
    }
    newBits._bb.position(wo);
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

  @Override protected void filterInitMap(ValueArray ary, Key k, int rows) {
    long row = ValueArray.getChunkIndex(k) * ValueArray.CHUNK_SZ / ary._rowsize;
    _bIter = new VAIterator(_bVect,_bCol,row);
  }

/*  @Override public int wire_len() {
    return super.wire_len()+_bVect.wire_len()+4;
  }

  @Override public void read(Stream s) {
    super.read(s);
    _bVect = Key.read(s);
    _bCol = s.get4();
  }

  @Override public void write(Stream s) {
    super.write(s);
    _bVect.write(s);
    s.set4(_bCol);
  }

  @Override public void read(DataInputStream ds) throws IOException {
    super.read(ds);
    _bVect = Key.read(ds);
    _bCol = ds.readInt();
  }

  @Override public void write(DataOutputStream ds) throws IOException {
    super.write(ds);
    _bVect.write(ds);
    ds.writeInt(_bCol);
  } */
}



