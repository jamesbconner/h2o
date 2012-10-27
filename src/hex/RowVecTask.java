package hex;

import water.*;

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
    if(dp == DataPreprocessing.NONE)return null;
    double [][] res = new double[colIds.length][2];
    if(dp != DataPreprocessing.NONE){
      switch(dp) {
      case NORMALIZE:
        for(int i = 0; i < colIds.length;++i){
          if(ary.col_max(colIds[i]) > 1 || ary.col_min(colIds[i]) < 0){
            double min = ary.col_min(colIds[i]);
            double max = ary.col_max(colIds[i]);
            res[i][1] = min;
            res[i][1] = 1/Math.max(Double.MAX_VALUE,max - min);
          }
        }
        break;
      case STANDARDIZE:
        for(int i = 0; i < colIds.length;++i){
          if(ary.col_mean(colIds[i]) != 0 || ary.col_sigma(colIds[i]) != 1){
            res[i][0] = ary.col_mean(colIds[i]);
            res[i][1] = 1/Math.max(Double.MIN_NORMAL, ary.col_sigma(colIds[i]));
          }
        }
        break;
      default:
        throw new Error("unknown DataPreprocessing mode " + dp);
      }
    }
    return res;
  }

  protected boolean _skipIncompleteLines; // if ture, rows with invalid/missing values will be skipped
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
    _ary = (ValueArray) DKV.get(aryKey);
//    if(_colIds == null && _colNames != null) {
//      _colIds = new int [_colNames.length];
//      _L0:for(int i = 0; i < _colIds.length; ++i) {
//        for(int j = 0; j < _ary.num_cols(); ++j) {
//          if(_colNames[i].equalsIgnoreCase(_ary.col_name(j))){
//            _colIds[i] = j;
//            continue _L0;
//          }
//        }
//        throw new Error("unknown column " + _colNames[i]);
//      }
//    }
    byte[] bits = DKV.get(key).get();
    int[] off = new int[_colIds.length];
    int[] sz = new int[_colIds.length];
    int[] base = new int[_colIds.length];
    int[] scale = new int[_colIds.length];
    for( int i = 0; i < _colIds.length; ++i ) {
      off[i] = _ary.col_off(_colIds[i]);
      sz[i] = _ary.col_size(_colIds[i]);
      base[i] = _ary.col_base(_colIds[i]);
      scale[i] = _ary.col_scale(_colIds[i]);
    }
    int row_size = _ary.row_size();
    int nrows = bits.length / row_size;
    double [] x = new double[_colIds.length];
    int c = _offset;
    preMap(x.length,nrows);
__OUTER:
    for( int rid = 0; rid < nrows; ++rid ) {
      if(_step != 0){
        if(--c <= 0)c += _step;
        if(((c == _step) && !_complement) || ((c != _step) && _complement))
          continue;
      }
      for( int i = 0; i < _colIds.length; ++i ) {
        if(_skipIncompleteLines && !_ary.valid(bits, rid, row_size, off[i], sz[i]))
         continue __OUTER;
        x[i] = _ary.datad(bits, rid, row_size, off[i], sz[i], base[i],scale[i], _colIds[i]);
        if(_pVals != null && _pVals[i][1] != 0) x[i] = (x[i] - _pVals[i][0]) * _pVals[i][1];
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
