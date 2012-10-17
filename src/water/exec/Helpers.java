
package water.exec;

import water.*;
import water.parser.ParseDataset;

/**
 *
 * @author peta
 */
public class Helpers {
  
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
  public static void assign(int pos, final Key to, Expr.Result what) throws EvaluationException {
    if( what.isConstant() ) { // assigning to a constant creates a vector of size 1 
      // The 1 tiny arraylet
      Key key2 = ValueArray.make_chunkkey(to, 0);
      byte[] bits = new byte[8];
      UDP.set8d(bits, 0, what._const);
      Value val = new Value(key2, bits);
      DKV.put(key2, val);
      // The metadata
      VABuilder b = new VABuilder(to.toString(),1).addDoubleColumn("0",what._const, what._const, what._const,0).createAndStore(to);
    } else if( what.canShallowCopy() ) {
      assert (false); // we do not support shallow copy now (TODO)
      ValueArray v = (ValueArray) DKV.get(what._key);
      if( v == null )
        throw new EvaluationException(pos, "Key " + what._key + " not found");
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to, MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to, r);
      what._copied = true; // TODO do we need to sync this? 
    } else if (what.colIndex()!=-1) { // copy in place of a single column only
      ValueArray v = (ValueArray) DKV.get(what._key);
      if( v == null )
        throw new EvaluationException(pos, "Key " + what._key + " not found");
      int col = what.colIndex();
      VABuilder b = new VABuilder(to.toString(), v.num_rows()).addColumn(v.col_name(col),v.col_size(col), v.col_scale(col),v.col_min(col), v.col_max(col), v.col_mean(col), v.col_sigma(col)).createAndStore(to);
      DeepColumnAssignment da = new DeepColumnAssignment(what._key,to, col);
      da.invoke(to);
    } else {
      ValueArray v = (ValueArray) DKV.get(what._key);
      if( v == null )
        throw new EvaluationException(pos, "Key " + what._key + " not found");
      byte[] bits = v.get();
      ValueArray r = new ValueArray(to, MemoryManager.arrayCopyOfRange(bits, 0, bits.length)); // we must copy it because of the memory managed
      DKV.put(to, r);
      MRTask copyTask = new MRTask() {

        @Override
        public void map(Key key) {
          byte[] bits = DKV.get(key).get();
          long offset = ValueArray.getOffset(key);
          Key k = ValueArray.make_chunkkey(to, offset);
          Value v = new Value(k, MemoryManager.arrayCopyOfRange(bits, 0, bits.length));
          DKV.put(k, v);
        }

        @Override
        public void reduce(DRemoteTask drt) {
          // pass
        }
      };
      copyTask.invoke(what._key);
    }
  }

}
