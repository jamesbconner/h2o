package hex;

import java.util.Iterator;
import java.util.NoSuchElementException;
import water.*;

public final class HexDataFrame extends Iced {
  int [] _colIds;
  Key _aryKey;
  transient ValueArray _ary;
  transient int [] _indexes;
  transient double [] _values;

  HexDataFrame(Key aryKey, int [] colIds){
    _aryKey = aryKey;
    _colIds = colIds;
  }

  int rowLen() {return _colIds.length;}



  public class ChunkData {
    int _n;
    AutoBuffer _bits;
    public ChunkData(byte [] bits){
      _bits = new AutoBuffer(bits);
      _n = bits.length/_ary._rowsize;
    }

    public final class HexRow {
      int _rid;

      boolean valid(int i){
        return !_ary.isNA(_bits, _rid, i);
      }
      public double getD(int i) {
        return _ary.datad(_bits, _rid, i);
      }

      public int getI(int i) {
        return (int)_ary.data(_bits, _rid,i);
      }

      public long getL(int i) {
        return (int)_ary.data(_bits, _rid,i);
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
    _ary = ValueArray.value(_aryKey);
  }

  public ChunkData getChunkData(Key k){
    return new ChunkData(DKV.get(k).get());
  }
}
