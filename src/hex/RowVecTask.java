package hex;

import hex.HexDataFrame.ChunkData;
import hex.HexDataFrame.ChunkData.HexRow;
import init.H2OSerializable;

import java.util.Arrays;

import water.*;

public abstract class RowVecTask extends MRTask {

  public enum DataPreprocessing {
    NONE,        // x_new = x
    NORMALIZE,   // x_new = (x - x_min)/(x_max - x_min)  /// scales numeric values to 0 - 1 range
    STANDARDIZE, // x_new = (x - x_mu)/x_sigma  /// transforms data to have zero mean and unit variance
    AUTO
  };

  @Override
  public void init(){
    super.init();
    _data.init();
  }

  public RowVecTask(HexDataFrame data){
    _data = data;
  }
  public static class Sampling implements H2OSerializable{
    final int _step;
    final int _offset;
    final boolean _complement;
    int _idx;

    public Sampling(int offset, int step, boolean complement){
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

    boolean skip(){
      return _idx++ % _step == _offset;
    }
    public String toString(){
      return "Sampling(step="+_step + ",offset=" + _offset + "complement=" + _complement + ")";
    }
  }

  protected boolean _skipIncompleteLines = true; // if true, rows with invalid/missing values will be skipped

  public RowVecTask() {}

  public RowVecTask(RowVecTask other){
    _skipIncompleteLines = other._skipIncompleteLines;
    _s = other._s;
  }



  public void setSampling(Sampling s){
    _s = s;
  }

  Sampling _s;
  HexDataFrame _data;
  int [] _categoricals;
  int [] _numeric;
  int [] _colOffsets;
  double [] _normSub;
  double [] _normMul;

  protected transient ValueArray _ary;
  @Override
  public void map(Key key) {
    ChunkData data = _data.getChunkData(key);
    init(data);
    double [] x = new double[_data._colIds.length];
    Arrays.fill(x, 1.0);
    int [] indexes = new int[x.length];
    // compute offsets
ROW:
    for(HexRow r:data.rows()){
      if(_s != null && _s.skip())continue;
      if(_categoricals != null) for(int i:_categoricals){
        if(!r.valid(i))continue ROW;
        indexes[i] = r.getI(i) + _colOffsets[i] + i;
        if(_normSub != null)
          x[i] = _normMul[indexes[i]];
        else
          x[i] = 1.0;
      }
      if(_numeric != null) for (int i:_numeric){
        if(!r.valid(i))continue ROW;
        x[i] = r.getD(i);
        indexes[i] = i + _colOffsets[i];
        if(_normSub != null)
          x[i] = (x[i] - _normSub[indexes[i]]) * _normMul[indexes[i]];
      }
      processRow(x, indexes);
    }
    // do not pass this back...
    _ary = null;
  }
  abstract void processRow(double [] x, int [] indexes);
  protected void init(ChunkData r){}
}
