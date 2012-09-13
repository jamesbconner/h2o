package hexlytics.rf;

import hexlytics.rf.Data.Row;
import hexlytics.rf.Statistic.Split;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class Data implements Iterable<Row> {

  public final class Row {
    int index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(index);
      sb.append(" ["+classOf()+"]:");
      for (int i = 0; i <data_.columns();i++)
        sb.append(" "+getS(i));
      return sb.toString();
    }
    public int numClasses() { return classes(); }
    public int classOf() { return  data_.classOf(index); }
    public float getF(int col) { return data_.getF(col,index); }
    public short getS(int col) { return data_.getS(index,col); }

    /** Support for binning information on the columns.  */
    public final int getColumnClass(int colIndex) {
      return data_.getColumnClass(index,colIndex);
    }
  }

  public class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    final int _start, _end;
    int _pos = 0;
    public RowIter(int start, int end) { _pos = _start = start; _end = end; }
    public boolean hasNext() { return _pos < _end; }
    public Row next() { _r.index = permute(_pos++); return _r; }
    public void remove() { throw new Error("Unsupported"); }
  }

  protected final DataAdapter data_;

  static final DecimalFormat df = new  DecimalFormat ("0.##");

  /** Returns new Data object that stores all adapter's rows unchanged.   */
  public static Data make(DataAdapter da) { return new Data(da); }

  protected Data(DataAdapter da) { data_ = da; }

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }

  public int columnClasses(int colIndex) { return data_.columnClasses(colIndex); }

  /** Returns the number of rows that is accessible by this Data object. */
  public int rows()        { return data_.rows(); }
  public int start()       { return 0; }
  public int end()         { return data_.rows(); }
  public  int columns()    { return data_.columns() -1 ; } // -1 to remove class column
  public  int classes()    { return data_.classes(); }
  public Random random()          { return data_.random_; }
  public String colName(int c)    { return data_.colName(c); }
  public double colMin(int c)     { return data_.colMin(c); }
  public double colMax(int c)     { return data_.colMax(c); }
  public double colTot(int c)     { return data_.colTot(c); }
  public  String[] columnNames()  { return data_.columnNames(); }
  public  String classColumnName(){ return data_.classColumnName(); }
  public String name() { return data_.name(); }
  public int last(int column) { return data_.c_[column].o2v2.size();}


  // subsets -------------------------------------------------------------------

  public void filter(Split c, Data[] result, Statistic[] stats) {
    int[] permuted = new int[rows()];
    int l = 0, r = permuted.length-1;
    for(Row row : this) {
      if (row.getS(c.column) < c.value) {
        stats[0].add(row);
        permuted[l++] = row.index;
      } else {
        stats[1].add(row);
        permuted[r--] = row.index;
      }
    }
    assert r+1 == l;
    result[0]= new Subset(this, permuted, 0, l);
    result[1]= new Subset(this, permuted, l, permuted.length);
  }

  public void filter(int column, int split, Data[] result, BaseStatistic left, BaseStatistic right) {
    assert getClass() == Data.class;

    int[] permuted = new int[rows()];
    int l = 0, r = permuted.length-1;

    final Row row = new Row();
    final int end = end();
    for( int pos = start(); pos < end; ++pos ) {
      row.index = pos;
      if (row.getColumnClass(column) <= split) {
        left.add(row);
        permuted[l++] = pos;
      } else {
        right.add(row);
        permuted[r--] = pos;
      }
    }

    assert r+1 == l;
    result[0]= new Subset(this, permuted, 0, l);
    result[1]= new Subset(this, permuted, l, permuted.length);
  }

  public Data sampleWithReplacement(double bagSizePct) {
    int[] sample = new int[(int)(rows() * bagSizePct)];
    Random r = new Random(data_.random_.nextLong());
    for (int i=0;i<sample.length;i++)
      sample[i] = permute(r.nextInt(rows()));
    Arrays.sort(sample); // make sure we access data in order
    return new Subset(this,sample,0,sample.length);
  }

  public Data complement() { throw new Error("Only for subsets."); }


  /** Returns the original index of the row in the DataAdapter object for row
   * indexed in the given Data object.  */
  protected int permute(int idx) { return idx; }

}

class Subset extends Data {
  private final int[] _permutation;
  private final int _start, _end;
  private final Data _parent;

  /** Returns the original DataAdapter's row index of given row. */
  @Override protected int permute(int idx) {
    assert _start <= idx && idx < _end;
    return _permutation[idx];
  }

  /** Returns the number of rows. */
  @Override public int rows()  { return _end - _start; }
  @Override public int start() { return _start; }
  @Override public int end()   { return _end; }

  // Totals, mins & maxs are not recomputed on the subsets.
  @Override public double colMin(int c)     { return data_.colMin(c); }
  @Override public double colMax(int c)     { return data_.colMax(c); }
  @Override public double colTot(int c)     { return Double.NaN; }

  /** Creates new subset of the given data adapter. The permutation is an array
   * of original row indices of the DataAdapter object that will be used.  */
  public Subset(Data data, int[] permutation, int start, int end) {
    super(data.data_);
    _start       = start;
    _end         = end;
    _parent      = data;
    _permutation = permutation;
  }

  @Override public void filter(int column, int split, Data[] result,
      BaseStatistic left, BaseStatistic right) {
    final Row row = new Row();
    int l = _start, r = _end - 1;
    while (l <= r) {
      int permIdx = row.index = _permutation[l];
      if (row.getColumnClass(column) <= split) {
        left.add(row);
        ++l;
      } else {
        right.add(row);
        _permutation[l] = _permutation[r];
        _permutation[r--] = permIdx;
      }
    }
    assert r+1 == l;
    result[0]= new Subset(this, _permutation, _start, l);
    result[1]= new Subset(this, _permutation, l, _end);
  }

  @Override public Data complement() {
    Set<Integer> s = new HashSet<Integer>();
    for (Row r : _parent) s.add(r.index);
    for (Row r : this) s.remove(r.index);
    int[] p = new int[s.size()];
    int i=0;
    for (Integer v : s) p[i++] = v.intValue();
    return new Subset(_parent,p,0,p.length);
  }
}