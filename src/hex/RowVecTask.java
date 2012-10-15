package hex;

import water.DKV;
import water.Key;
import water.MRTask;
import water.ValueArray;

public abstract class RowVecTask extends MRTask {

  public static final int NO_PREPROCESSING = 0; // x_new = x
  public static final int NORMALIZE_DATA   = 1; // x_new = (x - x_min)/(x_max - x_min)  /// scales numeric values to 0 - 1 range
  public static final int STANDARDIZE_DATA = 2; // x_new = (x - x_mu)/x_sigma  /// transforms data to have zero mean and unit variance

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
  protected int [] _p = null;//NO_PREPROCESSING;

  protected boolean _skipInvalidLines; // if ture, rows with invalid/missing values will be skipped

  protected int [] _colIds;
  long _n;
  public RowVecTask() {}
  public RowVecTask(int [] colIds, boolean skipInvalidLines, int [] p){this(colIds,null, skipInvalidLines,p);}
  public RowVecTask(int [] colIds, Sampling s, boolean skipInvalidLines, int [] p){
    _skipInvalidLines = skipInvalidLines;
    _p = p;
    _colIds = colIds;
    if(s != null){
      _offset = s._offset;
      _step = s._step;
      _complement = s._complement;
    }
  }
  public RowVecTask(RowVecTask other){
    _skipInvalidLines = other._skipInvalidLines;
    _p = other._p;
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
  @Override
  public void map(Key key) {
    assert key.home();
    Key aryKey = Key.make(ValueArray.getArrayKeyBytes(key));
    ValueArray ary = (ValueArray) DKV.get(aryKey);
    byte[] bits = DKV.get(key).get();
    int[] off = new int[_colIds.length];
    int[] sz = new int[_colIds.length];
    int[] base = new int[_colIds.length];
    int[] scale = new int[_colIds.length];
    for( int i = 0; i < _colIds.length; ++i ) {
      off[i] = ary.col_off(_colIds[i]);
      sz[i] = ary.col_size(_colIds[i]);
      base[i] = ary.col_base(_colIds[i]);
      scale[i] = ary.col_scale(_colIds[i]);
    }
    int row_size = ary.row_size();
    int nrows = bits.length / row_size;
    double [] x = new double[_colIds.length];
    int c = _offset;
    init(x.length,nrows);
__OUTER:
    for( int rid = 0; rid < nrows; ++rid ) {
      if(_step != 0){
        if(--c <= 0)c += _step;
        if(((c == _step) && !_complement) || ((c != _step) && _complement))
          continue;
      }
      for( int i = 0; i < _colIds.length; ++i ) {
        if(_skipInvalidLines && !ary.valid(bits, rid, row_size, off[i], sz[i]))
         continue __OUTER;
        x[i] = ary.datad(bits, rid, row_size, off[i], sz[i], base[i],scale[i], _colIds[i]);
        if(_p != null)
          switch(_p[i]){
          case NORMALIZE_DATA:
            x[i] = (x[i] - ary.col_min(_colIds[i]))/(ary.col_max(_colIds[i]) - x[i]);
            break;
          case STANDARDIZE_DATA:
            x[i] = (x[i] - ary.col_mean(_colIds[i]))/ary.col_sigma(_colIds[i]);
            break;
          case NO_PREPROCESSING:
            break;
          default:
            throw new Error("Illegal preprocessing flag " + _p);
          }
      }
      ++_n;
      map(x);
    }
    cleanup();
  }
  abstract void map(double [] x);
  protected void init(int xlen, int nrows){}
  protected void cleanup() {}
}
