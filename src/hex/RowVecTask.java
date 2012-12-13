package hex;

import water.*;
import water.ValueArray.Column;

public abstract class RowVecTask extends MRTask {

  public enum DataPreprocessing {
    NONE,        // x_new = x
    NORMALIZE,   // x_new = (x - x_min)/(x_max - x_min)  /// scales numeric values to 0 - 1 range
    STANDARDIZE, // x_new = (x - x_mu)/x_sigma  /// transforms data to have zero mean and unit variance
    AUTO
  };

  public static class Sampling {
    final int _step;
    final int _offset;
    final boolean _complement;

    public Sampling(int offset, int step, boolean complement){
      _step = step;
      _complement = complement;
      _offset = offset;
    }

    Sampling complement(){
      return new Sampling(_offset,_step, !_complement);
    }
  }

  public static double[][] getDataPreprocessingForColumns(DataPreprocessing dp, ValueArray ary, int [] colIds){
    if( dp == DataPreprocessing.NONE ) return null;
    double[][] res = new double[colIds.length][2];
    switch(dp) {
    case NORMALIZE:
      for( int i = 0; i < colIds.length; ++i) {
        Column c = ary._cols[colIds[i]];
        if(c._max > 1 || c._min < 0){
          double min = c._min;
          double max = c._max;
          res[i][0] = min;
          res[i][1] = max == min ? 1 : 1/(max - min);
        }
      }
      break;
    case STANDARDIZE:
      for( int i = 0; i < colIds.length;++i ) {
        Column c = ary._cols[colIds[i]];
        if( c._domain == null || c._domain.length == 0 ) res[i][1] = 0;
        else if( c._mean != 0 || c._sigma != 1 ) {
          res[i][0] = c._mean;
          res[i][1] = 1/Math.max(Double.MIN_NORMAL, c._sigma);
        }
      }
      break;
    default: throw new Error("unknown DataPreprocessing mode " + dp);
    }
    return res;
  }

  protected boolean _skipIncompleteLines; // if true, rows with invalid/missing values will be skipped
  protected int [] _colIds;
  protected double [][] _pVals;
  long _n;
  public RowVecTask() {}

  public RowVecTask(Sampling s){this(null,s,false,null);}

  public RowVecTask(int [] colIds, boolean skipInvalidLines, double[][] pVals){this(colIds,null, skipInvalidLines,pVals);}
  public RowVecTask(int [] colIds, Sampling s, boolean skipInvalidLines, double[][] pVals){
    _skipIncompleteLines = skipInvalidLines;
    _pVals = pVals;
    _colIds = colIds;
    if(s != null){
      _offset = s._offset;
      _step = s._step;
      _complement = s._complement;
    }
  }
  public RowVecTask(RowVecTask other){
    _skipIncompleteLines = other._skipIncompleteLines;
    _pVals = other._pVals;
    _colIds = other._colIds; _step = other._step; _offset = other._offset; _complement = other._complement;

  }
  int _step = 0;
  int _offset = 0;
  boolean _complement;

  /**
   * Call this if you want this task to be sampled. Approximately ratio*N rows will be selected.
   * If complement is true, (1 - ratio)*N will be selected.
   *
   * Gives deterministic results given by the seed.
   *
   * So e.g. setSampling(0,0.5,false) and setSampling(0,0.5,true) are exact complements of approximately same size.
   *
   * @param seed seed for the random number generator
   * @param ratio value in range 0 - 1 giving the ratio of rows to be selected. 0 means no row will be selcted, 1 means all rows will be selected.
   * @param complement - if true, returns exactly the complement of the set defined by the seed and ratio.
   */
  public void setSampling(Sampling s){
    if(s != null){
      _offset = s._offset;
      _step = s._step;
      _complement = s._complement;
    }
  }

  protected transient ValueArray _ary;

  @Override
  public void map(Key key) {
    assert key.home();
    Key aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
    _ary = ValueArray.value(DKV.get(aryKey));
    AutoBuffer bits  = _ary.get_chunk(key);
    ValueArray.Column[] cols = new ValueArray.Column[_colIds.length];
    for( int i = 0; i < _colIds.length; ++i ) {
      cols[i] = _ary._cols[_colIds[i]];
    }
    final int nrows = bits.remaining()/_ary._rowsize;

    double[] x = new double[_colIds.length];
    int c = _offset;
    preMap(x.length,nrows);

    ROWS: for( int rid = 0; rid < nrows; ++rid ) {
      if( _step != 0 ) {
        if( --c <= 0 ) c += _step;
        if(((c == _step) && !_complement) || ((c != _step) && _complement))
          continue;
      }
      for( int i = 0; i < _colIds.length; ++i) {
        if(_skipIncompleteLines && _ary.isNA(bits, rid, cols[i]))
          continue ROWS;
        x[i] = _ary.datad(bits, rid, cols[i]);
        if( _pVals != null && _pVals[i][1] != 0 )
          x[i] = (x[i] - _pVals[i][0]) * _pVals[i][1];
      }
      ++_n;
      processRow(x);
    }
    postMap();
    // do not pass this back...
    _ary = null;
  }
  abstract void processRow(double [] x);
  protected void preMap(int xlen, int nrows){}
  protected void postMap() {}
}
