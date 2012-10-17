package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Tree.SplitNode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import com.google.common.primitives.Ints;

public class Data implements Iterable<Row> {

  public final class Row {
    int index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(index).append(" ["+classOf()+"]:");
      for( int i = 0; i < data_.columns(); ++i ) sb.append(" "+getS(i));
      return sb.toString();
    }
    public int numClasses()    { return classes(); }
    public int classOf()       { return  data_.classOf(index); }
    public float getF(int col) { return data_.getF(col,index); }
    public short getS(int col) { return data_.getS(index,col); }

    public final short getEncodedColumnValue(int colIndex) {
      return data_.getEncodedColumnValue(index, colIndex);
    }
  }

  protected final DataAdapter data_;

  /** Returns new Data object that stores all adapter's rows unchanged.   */
  public static Data make(DataAdapter da) { return new Data(da); }

  protected Data(DataAdapter da) { data_ = da; }

  protected int start()          { return 0;                   }
  protected int end()            { return data_.rows();        }
  public int rows()              { return end() - start();     }
  public int columns()           { return data_.columns();     }
  public int classes()           { return data_.classes();     }
  public int seed()              { return data_.seed();        }
  public int dataId()            { return data_.dataId();      }
  public int classIdx()          { return data_.classIdx_;     }
  public String colName(int i)   { return data_.columnNames()[i]; }
  public float unmap(int col, float split) { return data_.unmap(col, split); }
  public int columnArity(int colIndex) { return data_.columnArity(colIndex); }

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }
  private class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    int _pos = 0; final int _end;
    public RowIter(int start, int end) { _pos = start; _end = end;       }
    public boolean hasNext()           { return _pos < _end;             }
    public Row next()                  { _r.index = permute(_pos++); return _r; }
    public void remove()               { throw new Error("Unsupported"); }
  }

  public void filter(SplitNode node, Data[] result, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int[] permutation = getPermutationArray();
    int l = start(), r = end() - 1;
    while (l <= r) {
      int permIdx = row.index = permutation[l];
      if (node.isIn(row)) {
        ls.add(row);
        ++l;
      } else {
        rs.add(row);
        permutation[l] = permutation[r];
        permutation[r--] = permIdx;
      }
    }
    assert r+1 == l;
    result[0]= new Subset(this, permutation, start(), l);
    result[1]= new Subset(this, permutation, l,   end());
  }

  public Data sampleWithReplacement(double bagSizePct, short[] complement) {
    // Make sure that values come in order
    short[] in = complement;
    int size = (int)(rows() * bagSizePct);
    Random r = new Random(seed());
    for( int i = 0; i < size; ++i)
      in[permute(r.nextInt(rows()))]++;
    int[] sample = new int[size];
    for( int i = 0, j = 0; i < sample.length;) {
      while(in[j]==0) j++;
      for (int k = 0; k < in[j]; k++) sample[i++] = j;
      j++;
    }
    return new Subset(this, sample, 0, sample.length);
  }

  public Data complement(Data parent, short[] complement) { throw new Error("Only for subsets."); }
  @Override public       Data clone() { return this; }

  protected int permute(int idx) { return idx; }
  protected int[] getPermutationArray() {
    int[] perm = new int[rows()];
    for( int i = 0; i < perm.length; ++i ) perm[i] = i;
    return perm;
  }
}

class Subset extends Data {
  private final int[] _permutation;
  private final int _start, _end;

  @Override protected int[] getPermutationArray() { return _permutation;      }
  @Override protected int permute(int idx)        { return _permutation[idx]; }
  @Override protected int start()                 { return _start;            }
  @Override protected int end()                   { return _end;              }
  @Override public Subset clone()                 { return new Subset(this,_permutation.clone(),_start,_end); }

  /** Creates new subset of the given data adapter. The permutation is an array
   * of original row indices of the DataAdapter object that will be used.  */
  public Subset(Data data, int[] permutation, int start, int end) {
    super(data.data_);
    _start       = start;
    _end         = end;
    _permutation = permutation;
  }

  @Override public Data complement(Data parent, short[] complement) {
    int size= 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) size++;
    int[] p = new int[size];
    int pos = 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) p[pos++] = i;
    return new Subset(this, p, 0, p.length);
  }
}
