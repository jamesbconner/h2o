
package hex;

import init.H2OSerializable;

import java.util.Iterator;
import java.util.NoSuchElementException;

import water.*;

public final class HexDataFrame implements H2OSerializable {
  int [] _colIds;
  Key _aryKey;
  transient ValueArray _ary;
  transient int [] _colIdxBases;
  transient int [] _off;
  transient int [] _base;
  transient int [] _scale;
  transient int [] _csize;
  transient int _rowsize;
  transient int [] _indexes;
  transient double [] _values;

  HexDataFrame(Key aryKey, int [] colIds){
    _aryKey = aryKey;
    _colIds = colIds;
  }

  int rowLen() {return _colIds.length;}



  public class ChunkData {
    int _n;
    byte [] _bits;
    public ChunkData(byte [] bits){
      _bits = bits;
      _n = _bits.length/_rowsize;
    }

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
    public Iterable<HexRow> rows(){
      return new Iterable<HexRow>() {
        @Override
        public Iterator<HexRow> iterator() {
          return ChunkData.this.iterator();
        }
      };
    }

    public Iterator<HexRow>  iterator(){
      final HexRow row = new HexRow();
      return new Iterator<HexRow>() {
        int _nextIdx = 0;
        @Override
        public boolean hasNext() {
          return _nextIdx < _n;
        }

        @Override
        public HexRow next() {
          if(!hasNext())throw new NoSuchElementException();
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

  public void init(){
    _ary = (ValueArray)DKV.get(_aryKey);
    _rowsize = _ary.row_size();
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
  }

  public ChunkData getChunkData(Key k){
    return new ChunkData(DKV.get(k).get());
  }
}
