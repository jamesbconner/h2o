package hex;

import java.util.Arrays;
import water.*;

public abstract class RowVecTask extends MRTask {
  Sampling _s;
  int [] _categoricals;
  int [] _numeric;
  int [] _colOffsets;
  double [] _normSub;
  double [] _normMul;

  int [] _colIds;
  Key _aryKey;
  transient ValueArray _ary;

  public enum DataPreprocessing {
    NONE,        // x_new = x
    NORMALIZE,   // x_new = (x - x_min)/(x_max - x_min)  /// scales numeric values to 0 - 1 range
    STANDARDIZE, // x_new = (x - x_mu)/x_sigma  /// transforms data to have zero mean and unit variance
    AUTO
  };

  // Set once per Node, shared ValueArray unpack
  @Override
  public void init(){
    super.init();
    _ary = ValueArray.value(_aryKey);
  }

  public RowVecTask(Key aryKey, int[] colIds){
    _aryKey = aryKey;
    _colIds = colIds;
  }

  public static class Sampling extends Iced {
    private final int _step;
    private final int _offset;
    private final boolean _complement;
    private int _next;

    public Sampling(int offset, int step, boolean complement) {
      _step = step;
      _complement = complement;
      _offset = offset;
    }

    Sampling complement(){
      return new Sampling(_offset,_step, !_complement);
    }

    public Sampling reset() {
      return new Sampling(_offset,_step,_complement);
    }

    boolean skip(int row) {
      if( row < _next+_offset ) return _complement;
      _next += _step;
      return !_complement;
    }
    public String toString(){
      return "Sampling(step="+_step + ",offset=" + _offset + "complement=" + _complement + ")";
    }
  }

  public RowVecTask() {}
  public RowVecTask(RowVecTask other){
    _s = other._s;
  }

  public void setSampling(Sampling s){
    _s = s;
  }

  @Override
  public void map(Key key) {
    init2();                    // Specialized subtask per-chunk init
    AutoBuffer bits = _ary.getChunk(key);
    final int rows = bits.remaining()/_ary._rowsize;
    double [] x = new double[_colIds.length];
    Arrays.fill(x, 1.0);
    int [] indexes = new int[x.length];
    // compute offsets
ROW:
    for( int r=0; r<rows; r++ ) {
      if( _s != null && _s.skip(r) ) continue;
      if( _categoricals != null ) for( int i : _categoricals ) {
        if( _ary.isNA(bits,r,i) ) continue ROW;
        indexes[i] = (int)_ary.data(bits,r,i) + _colOffsets[i] + i;
        if(_normSub != null)
          x[i] = _normMul[indexes[i]];
        else
          x[i] = 1.0;
      }
      if(_numeric != null) for (int i:_numeric){
        int col = _colIds[i];
        if( _ary.isNA(bits,r,col) ) continue ROW;
        x[i] = _ary.datad(bits,r,col);
        indexes[i] = i + _colOffsets[i];
        if(_normSub != null)
          x[i] = (x[i] - _normSub[indexes[i]]) * _normMul[indexes[i]];
      }
      processRow(x, indexes);
    }
  }
  abstract void processRow(double [] x, int [] indexes);
  protected void init2(){}
}
