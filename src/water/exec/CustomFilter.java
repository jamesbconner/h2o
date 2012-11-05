
package water.exec;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import water.*;


/**
 *
 * @author peta
 */
public abstract class CustomFilter extends MRTask {
  
  long _filteredRows;

  Key _destKey;
  
  
  protected CustomFilter(Key destKey) {
    _destKey = destKey;
  }
  
  @Override public void map(Key key) {
    ValueArray ary = (ValueArray) DKV.get(Key.make(ValueArray.getArrayKeyBytes(key)));
    byte[] bits = DKV.get(key).get();
    byte[] newBits = MemoryManager.allocateMemory(bits.length);
    int wo = 0;
    int rowSize = ary.row_size();
    filterInitMap(ary,key,bits.length);
    for (int offset = 0; offset < bits.length; offset += rowSize) {
      if (filter(bits,offset)) {
        ++_filteredRows;
        System.arraycopy(bits,offset,newBits,wo,rowSize);
        wo += rowSize;
      }
    }
    byte[] x = MemoryManager.allocateMemory(wo);
    System.arraycopy(newBits,0,x,0,wo);
    Key d = ValueArray.make_chunkkey(_destKey, ValueArray.getOffset(key));
    Value v = new Value(d,x);
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
  protected abstract boolean filter(byte[] bits, int rowOffset);
  
  /** Override this if you need some code to be called when map is
   * 
   * @param rows 
   */
  protected void filterInitMap(ValueArray ary, Key k, int rows) {
    // pass
  }

}


// =============================================================================
// RandomFilter
// =============================================================================

class RandomFilter extends CustomFilter {

  long resultRows;
  
  @Override protected boolean filter(byte[] bits, int rowOffset) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override protected void filterInitMap(ValueArray ary, Key k, int rows) {
    // pass
  }
  
  protected RandomFilter(Key destKey) {
    super(destKey);
  }
  
}

// =============================================================================
// BooleanVectorFilter
// =============================================================================

class BooleanVectorFilter extends CustomFilter {
  
  Key _bVect; 
  int _bCol;
  VAIterator _bIter;
  
  protected BooleanVectorFilter(Key destKey, Key bVect, int bCol) {
    super(destKey);
    _bVect = bVect;
    _bCol = bCol;
  }
  
  @Override protected boolean filter(byte[] bits, int rowOffset) {
    _bIter.next();
    return _bIter.datad() != 0;
  }

  @Override protected void filterInitMap(ValueArray ary, Key k, int rows) {
    long row = ValueArray.getOffset(k) / ary.row_size();
    _bIter = new VAIterator(_bVect,_bCol,row);
  }
  
  @Override public int wire_len() {
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
  }
}



