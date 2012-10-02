package hex;

import water.*;

public abstract class RowVecTask extends MRTask {

  protected int [] _colIds;

  public RowVecTask() {}
  public RowVecTask(int [] colIds){_colIds = colIds;}
  public RowVecTask(RowVecTask other){_colIds = other._colIds;}

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
    // _rmap gets shared among threads -> create thread's private copy
    int row_size = ary.row_size();
    int nrows = bits.length / row_size;
    double [] x = new double[_colIds.length];
    init(x.length,nrows);
    for( int rid = 0; rid < nrows; ++rid ) {
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
