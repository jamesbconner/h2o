package hex;

import java.util.Random;

import water.*;

public abstract class RowVecTask extends MRTask {
  protected int [] _colIds;
  transient Random _rand = null;
  public RowVecTask() {}
  public RowVecTask(int [] colIds){_colIds = colIds;}
  public RowVecTask(RowVecTask other){_colIds = other._colIds;}

  long _seed = System.currentTimeMillis();
  double _samplingRatio = 1.0;
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
  public void setSampling(long seed, double ratio, boolean complement){
    _seed = seed;
    _samplingRatio = ratio;
    _complement = complement;
  }
  @Override
  public void map(Key key) {
    if(_samplingRatio != 1.0){
      _rand = new Random(_seed);
    }
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
    // _rmap gets shared among threads -> create thread's private copy
    int row_size = ary.row_size();
    int nrows = bits.length / row_size;
    double [] x = new double[_colIds.length];
    init(x.length,nrows);
    for( int rid = 0; rid < nrows; ++rid ) {
      if(_samplingRatio < 1.0){
        double r = _rand.nextDouble();
        if((_complement && (r <= _samplingRatio)) || (!_complement && (r > _samplingRatio))){
          continue;
        }
      }
      for( int i = 0; i < _colIds.length; ++i )
        x[i] = ary.datad(bits, rid, row_size, off[i], sz[i], base[i],scale[i], _colIds[i]);
      map(x);
    }
    cleanup();
  }
  abstract void map(double [] x);
  protected void init(int xlen, int nrows){}
  protected void cleanup() {}
}
