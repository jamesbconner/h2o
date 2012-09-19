package hexlytics.rf;

import hexlytics.rf.Data.Row;

import java.util.*;

import com.google.common.primitives.Ints;

public class Data implements Iterable<Row> {

  public final class Row {
    int index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(index);
      sb.append(" ["+classOf()+"]:");
      for( int i = 0; i < data_.columns(); ++i ) sb.append(" "+getS(i));
      return sb.toString();
    }
    public int numClasses() { return classes(); }
    public int classOf() { return  data_.classOf(index); }
    public float getF(int col) { return data_.getF(col,index); }
    public short getS(int col) { return data_.getS(index,col); }

    /** Support for binning information on the columns.  */
    public final int getColumnClass(int colIndex) {
      return data_.getColumnClass(index, colIndex);
    }
  }

  protected final DataAdapter data_;

  /** Returns new Data object that stores all adapter's rows unchanged.   */
  public static Data make(DataAdapter da) { return new Data(da); }

  protected Data(DataAdapter da) { data_ = da; }


  protected int start()            { return 0;                            }
  protected int end()              { return data_.rows();                 }
  public int rows()                { return end() - start();              }
  public int columns()             { return data_.columns() -1 ; } // -1 to remove class column
  public int classes()             { return data_.classes();              }
  public int seed()                { return data_.seed();                 }
  
  public int columnClasses(int colIndex) { return data_.columnClasses(colIndex); }

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }
  private class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    int _pos = 0; final int _end;
    public RowIter(int start, int end) { _pos = start; _end = end; }
    public boolean hasNext() { return _pos < _end; }
    public Row next() { _r.index = permute(_pos++); return _r; }
    public void remove() { throw new Error("Unsupported"); }
  }
  
  public void filter(int column, int split, Data[] result, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int[] permutation = getPermutationArray();
    int l = start(), r = end() - 1;
    while (l <= r) {
      int permIdx = row.index = permutation[l];
      if (row.getColumnClass(column) <= split) {
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

  public void filterExclude(int column, int split, Data[] result, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int[] permutation = getPermutationArray();
    int l = start(), r = end() - 1;
    while (l <= r) {
      int permIdx = row.index = permutation[l];
      if (row.getColumnClass(column) == split) {
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

  public Data sampleWithReplacement(double bagSizePct) {
    int[] sample = new int[(int)(rows() * bagSizePct)];
    Random r = new Random(seed());
    for( int i = 0; i < sample.length; ++i)
      sample[i] = permute(r.nextInt(rows()));
    Arrays.sort(sample); // make sure we access data in order
    return new Subset(this, sample, 0, sample.length);
  }

  public Data complement(Data parent) { throw new Error("Only for subsets."); }

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
  
  /** Creates new subset of the given data adapter. The permutation is an array
   * of original row indices of the DataAdapter object that will be used.  */
  public Subset(Data data, int[] permutation, int start, int end) {
    super(data.data_);
    _start       = start;
    _end         = end;
    _permutation = permutation;
  }

  @Override public Data complement(Data parent) {
    Set<Integer> s = new HashSet<Integer>();
    for( Row r : parent ) s.add(r.index);
    for( Row r : this    ) s.remove(r.index);
    int[] p = Ints.toArray(s);
    return new Subset(this, p, 0, p.length);
  }
}
