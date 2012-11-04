
package water.exec;

import java.io.IOException;
import water.*;
import water.exec.Expr.Result;
import water.parser.ParseDataset;

/**
 *
 * @author peta
 */
public class Helpers {

  // scalar collector task -----------------------------------------------------
  
  public static abstract class ScallarCollector extends MRTask {

    public final Key _key;
    public int _col;
    protected double _result;

    protected abstract void collect(double x);
    
    protected abstract void reduce(double x);
    
    public double result() { return _result; }
    
    @Override public void map(Key key) {
      _result = 0;
      ValueArray va = (ValueArray) DKV.get(_key);
      double mean = va.col_mean(_col);
      Value v = DKV.get(key);
      if (v == null) 
        System.err.println(key.toString());
      byte[] bits = DKV.get(key).get();
      int rowSize = va.row_size();
      for( int i = 0; i < bits.length / rowSize; ++i ) {
        double x = va.datad(bits, i, rowSize, _col);
        if (!Double.isNaN(x))
          collect(x);
      }
    }

    @Override public void reduce(DRemoteTask drt) {
      Helpers.ScallarCollector other = (Helpers.ScallarCollector) drt;
      if (!Double.isNaN(other._result))
        reduce(other._result);
    }

    public ScallarCollector(Key key, int col, double initVal) { // constructor
      _key = key;
      _col = col >=0 ? col : 0;
      _result = initVal;
    }
  }
  
  
  
  
  // sigma ---------------------------------------------------------------------
  
  /**
   * Calculates the second pass of column metadata for the given key.
   *
   * Assumes that the min, max and mean are already calculated. gets the sigma
   *
   * @param key
   */
  public static void calculateSigma(final Key key, int col) {
    SigmaCalc sc = new SigmaCalc(key, col);
    sc.invoke(key);
    byte[] bits = DKV.get(key).get();
    ValueArray va = new ValueArray(key, MemoryManager.arrayCopyOfRange(bits, 0, bits.length));
    va.set_col_sigma(col, sc.sigma());
    DKV.put(key, va);
  }

  static class SigmaCalc extends MRTask {

    public final Key _key;
    public int _col;
    public double _sigma; // std dev

    @Override
    public void map(Key key) {
      ValueArray va = (ValueArray) DKV.get(_key);
      double mean = va.col_mean(_col);
      Value v = DKV.get(key);
      if (v == null) 
        System.err.println(key.toString());
      byte[] bits = DKV.get(key).get();
      int rowSize = va.row_size();
      for( int i = 0; i < bits.length / rowSize; ++i ) {
        double x = va.datad(bits, i, rowSize, _col);
        _sigma += (x - mean) * (x - mean);
      }
    }

    @Override
    public void reduce(DRemoteTask drt) {
      SigmaCalc other = (SigmaCalc) drt;
      _sigma += other._sigma;
    }

    public SigmaCalc(Key key, int col) { // constructor
      _key = key;
      _col = col;
      _sigma = 0;
    }

    public double sigma() {
      ValueArray va = (ValueArray) DKV.get(_key);
      return Math.sqrt(_sigma / va.num_rows());
    }
  }
  
  // ---------------------------------------------------------------------------
  // Assignments
  
  /**
   * Assigns (copies) the what argument to the given key.
   *
   * TODO at the moment, only does deep copy.
   *
   * @param to
   * @param what
   * @throws EvaluationException
   */
  public static void assign(int pos, final Key to, Result what) throws EvaluationException {
    if( what._type == Result.Type.rtNumberLiteral ) { // assigning to a constant creates a vector of size 1 
      // The 1 tiny arraylet
      Key key2 = ValueArray.make_chunkkey(to, 0);
      byte[] bits = new byte[8];
      UDP.set8d(bits, 0, what._const);
      Value val = new Value(key2, bits);
      TaskPutKey tpk = DKV.put(key2, val);
      // The metadata
      VABuilder b = new VABuilder(to.toString(),1).addDoubleColumn("0",what._const, what._const, what._const,0).createAndStore(to);
      if( tpk != null ) tpk.get();
    } else if (what._type == Result.Type.rtKey) {
      if( what.canShallowCopy() ) {
        assert (false); // we do not support shallow copy now (TODO)
        ValueArray v = (ValueArray) DKV.get(what._key);
        if( v == null )
          throw new EvaluationException(pos, "Key " + what._key + " not found");
        byte[] bits = v.get();
        ValueArray r = new ValueArray(to, MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
        DKV.put(to, r);
        what._copied = true; // TODO do we need to sync this? 
      } else if (what.rawColIndex()!=-1) { // copy in place of a single column only
        ValueArray v = (ValueArray) DKV.get(what._key);
        if( v == null )
          throw new EvaluationException(pos, "Key " + what._key + " not found");
        int col = what.rawColIndex();
        VABuilder b = new VABuilder(to.toString(), v.num_rows()).addColumn(v.col_name(col),v.col_size(col), v.col_scale(col),v.col_min(col), v.col_max(col), v.col_mean(col), v.col_sigma(col)).createAndStore(to);
        DeepSingleColumnAssignment da = new DeepSingleColumnAssignment(what._key, to, col);
        da.invoke(to);
      } else {
        ValueArray v = (ValueArray) DKV.get(what._key);
        if( v == null )
          throw new EvaluationException(pos, "Key " + what._key + " not found");
        byte[] bits = v.get();
        ValueArray r = new ValueArray(to, MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
        MRTask copyTask = new MRTask() {
          @Override public void map(Key key) {
            byte[] bits = DKV.get(key).get();
            long offset = ValueArray.getOffset(key);
            Key k = ValueArray.make_chunkkey(to, offset);
            Value v = new Value(k, MemoryManager.arrayCopyOfRange(bits, 0, bits.length));
            lazy_complete(DKV.put(k, v));
          }
          @Override  public void reduce(DRemoteTask drt) { }
        };
        copyTask.lazy_complete(DKV.put(to, r));
        copyTask.invoke(what._key);
      }
    } else {
      throw new EvaluationException(pos,"Only Values and numeric constants can be assigned");
    }
  }

  // sigma ---------------------------------------------------------------------

  /** Creates a simple vector using the given values only. 
   * 
   * @param name
   * @param items 
   */
  public void createVector(Key name, String colName, double[] items) {
    // TODO TODO TODO
    VABuilder b = new VABuilder(name.toString(), items.length).addDoubleColumn(colName);
    ValueArray va = b.create(name);
    byte[] bits = null;
    int offset = 0;
    double min = Double.MAX_VALUE;
    double max = -Double.MAX_VALUE;
    double tot = 0;
    for (int i = 0; i < items.length; ++i) {
      if ((bits == null) || (offset == bits.length)) { // create new chunk
        offset = 0;
        
      }
      UDP.set8d(bits,offset,items[i]);
      offset += 8;
      if (items[i] < min)
        min = items[i];
      if (items[i] > max)
        max = items[i];
      tot += items[i];
    }
    tot = tot / items.length;
    b.setColumnStats(0,min,max,tot);
    b.createAndStore(name);
  }
  
}


/** TODO scaling is missing, I should probably do VA iterators to do the job
 * for me much better. 
 * 
 * TODO!!!!!!!!!!!!
 * 
 * @author peta
 */
class DeepSingleColumnAssignment extends MRTask {

  private Key _to;
  private Key _from;
  private int _colIndex;
  
  
  @Override public void map(Key key) {
    ValueArray vTo = (ValueArray) DKV.get(_to);
    ValueArray vFrom = (ValueArray) DKV.get(_from);
    int colSize = vFrom.col_size(_colIndex);
    assert (colSize == vTo.col_size(0));
    long chunkOffset = ValueArray.getOffset(key);
    long row = chunkOffset / vTo.row_size();
    long chunkRows = ValueArray.chunk_size() / vTo.row_size(); // now rows per chunk
    if( row / chunkRows == vTo.chunks() - 1 )
      chunkRows = vTo.num_rows() - row;
    byte[] bits = MemoryManager.allocateMemory((int) chunkRows * vTo.row_size());
    int offset = 0;
    try {
    for (int i = 0; i < chunkRows; ++i) {
      switch (colSize) {
        case 1:
          bits[offset] = (byte) vFrom.data(i+row,_colIndex);
          offset += 1;
          break;
        case 2:
          UDP.set2(bits,offset, (int) vFrom.data(i+row,_colIndex));
          offset += 2;
          break;
        case 4:
          UDP.set4(bits,offset, (int) vFrom.data(i+row,_colIndex));
          offset += 4;
          break;
        case 8:
          UDP.set8(bits,offset, vFrom.data(i+row,_colIndex));
          offset += 8;
          break;
        case -8:
          UDP.set8d(bits,offset, vFrom.datad(i+row,_colIndex));
          offset += 8;
          break;
        default:
          throw new IOException("Unsupported colSize "+colSize);
      }
    }    
    } catch (IOException e) {
      e.printStackTrace();
    }
    // we have the bytes now, just store the value
    Value val = new Value(key, bits);
    lazy_complete(DKV.put(key, val));
    // and we are done...
    
  }

  @Override public void reduce(DRemoteTask drt) { }
  
  
  public DeepSingleColumnAssignment(Key from, Key to, int colIndex) {
    _to = to;
    _from = from;
    _colIndex = colIndex;
  }
  
}

