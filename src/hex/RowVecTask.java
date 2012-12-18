package hex;

import hex.HexDataFrame.HexRow;
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
  }

  public static double[][] getDataPreprocessingForColumns(DataPreprocessing dp, ValueArray ary, int [] colIds){
    if( dp == DataPreprocessing.NONE ) return null;
    double [][] res = new double[colIds.length][2];
    switch(dp) {
    case NORMALIZE:
      for(int i = 0; i < colIds.length;++i){
        if(ary.col_max(colIds[i]) > 1 || ary.col_min(colIds[i]) < 0){
          double min = ary.col_min(colIds[i]);
          double max = ary.col_max(colIds[i]);
          res[i][0] = min;
          res[i][1] = max == min ? 1 : 1/(max - min);
        }
      }
      break;
    case STANDARDIZE:
      for(int i = 0; i < colIds.length;++i){
        if(ary.col_has_enum_domain(i)) // do no standardize enums at the moment
          res[i][1] = 0;
        else if(ary.col_mean(colIds[i]) != 0 || ary.col_sigma(colIds[i]) != 1){
          res[i][0] = ary.col_mean(colIds[i]);
          res[i][1] = 1/Math.max(Double.MIN_NORMAL, ary.col_sigma(colIds[i]));
        }
      }
      break;
    default: throw new Error("unknown DataPreprocessing mode " + dp);
    }
    return res;
  }

  protected boolean _skipIncompleteLines = true; // if true, rows with invalid/missing values will be skipped
  protected int [] _colIds;

  long _n;
  public RowVecTask() {}

  public RowVecTask(Sampling s){this(null,s,false,null);}

  public RowVecTask(int [] colIds, boolean skipInvalidLines, double[][] pVals){this(colIds,null, skipInvalidLines,pVals);}
  public RowVecTask(int [] colIds, Sampling s, boolean skipInvalidLines, double[][] pVals){
    _skipIncompleteLines = skipInvalidLines;
    _colIds = colIds;
    _s = s;
  }
  public RowVecTask(RowVecTask other){
    _skipIncompleteLines = other._skipIncompleteLines;
    _colIds = other._colIds;
    _s = other._s;
  }

  Sampling _s;

  public void setSampling(Sampling s){
    _s = s;
  }

  HexDataFrame _data;

  int [] _categoricals;
  int [] _numeric;
  int [] _colOffsets;

  double [] _normSub;
  double [] _normMul;

  protected transient ValueArray _ary;
  @Override
  public void map(Key key) {
    _data.init(key);
    init(_data);
    double [] x = new double[_data._colIds.length];
    Arrays.fill(x, 1.0);
    int [] indexes = new int[x.length];
    // compute offsets
ROW:
    for(HexRow r:_data.rows()){
      if(_s != null && _s.skip())continue;
      if(_categoricals != null) for(int i:_categoricals){
        if(!r.valid(i))continue ROW;
        indexes[i] = r.getI(i) + _colOffsets[i];
        if(_normSub != null)
          x[i] = _normSub[indexes[i]];
      }
      if(_numeric != null) for (int i:_numeric){
        if(!r.valid(i))continue ROW;
        x[i] = r.getD(i);
        indexes[i] = i + _colOffsets[i];
        if(_normSub != null)
          x[i] = x[i] - _normSub[indexes[i]] * _normMul[indexes[i]];
      }
      processRow(x, indexes);
    }
    // do not pass this back...
    _ary = null;
  }
  abstract void processRow(double [] x, int [] indexes);
  protected void init(HexDataFrame data){}
}
