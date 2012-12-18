
package hex;

import init.H2OSerializable;

import java.util.Iterator;

import water.*;

public final class HexDataFrame implements H2OSerializable {
  int [] _colIds;
  transient ValueArray _ary;
  transient byte [] _bits;
  transient int [] _colIdxBases;
  transient int [] _off;
  transient int [] _base;
  transient int [] _scale;
  transient int [] _csize;
  transient int _rowsize;
  transient int _nrows;
  transient int [] _indexes;
  transient double [] _values;

  HexDataFrame(int [] colIds){
    _colIds = colIds;
  }

  int rowLen() {return _colIds.length;}

  public final class HexRow {
    int _rid;

    boolean valid(int i){
      return _ary.valid(_bits, _rid, _rowsize, _off[i], _csize[i]);
    }
    public double getD(int i) {
      return _ary.datad(_bits, _rid, _rowsize, _off[i], _csize[i], _base[i], _scale[i], _colIds[i]);
    }

    public int getI(int i) {
      return (int)_ary.data(_bits, _rid, _rowsize, _off[i], _csize[i], _base[i], _scale[i], _colIds[i]);
    }

    public long getL(int i) {
      return _ary.data(_bits, _rid, _rowsize, _off[i], _csize[i], _base[i], _scale[i], _colIds[i]);
    }
    public double last(){
      return getD(_colIds.length-1);
    }

  }

  public void init(Key k){
    _ary = (ValueArray)DKV.get(Key.make(ValueArray.getArrayKeyBytes(k)));
    _bits = DKV.get(k).get();
    _off = new int[_colIds.length];
    _base = new int[_colIds.length];
    _scale = new int[_colIds.length];
    _csize = new int[_colIds.length];
    int i = 0;
    for(int c:_colIds){
      _off[i] = _ary.col_off(c);
      _base[i] = _ary.col_base(c);
      _scale[i] = _ary.col_scale(c);
      _csize[i] = _ary.col_size(c);
      ++i;
    }
    _rowsize = _ary.row_size();
    _nrows = _bits.length/_rowsize;
  }

  public Iterable<HexRow> rows() {
    return new Iterable<HexRow>() {
      @Override
      public Iterator<HexRow> iterator() {
        return HexDataFrame.this.iterator();
      }
    };
  }
  public Iterator<HexRow>  iterator(){
    final int N = _nrows;
    int idx = 0;
    final int indexFrom = idx;
    final HexRow row = new HexRow();
    return new Iterator<HexRow>() {
      int _nextIdx = indexFrom;
      @Override
      public boolean hasNext() {
        return _nextIdx < N;
      }

      @Override
      public HexRow next() {
        row._rid = _nextIdx++;
        return row;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }





}
