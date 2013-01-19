package hex.rf;

import hex.rf.Data.Row;
import hex.rf.Tree.SplitNode;

import java.util.*;

import water.MemoryManager;

public class Data implements Iterable<Row> {
  boolean _stratify;
  public final class Row {
    int _index;
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(_index).append(" ["+classOf()+"]:");
      for( int i = 0; i < _data.columns(); ++i ) sb.append(_data.getEncodedColumnValue(_index, i)).append(',');
      return sb.toString();
    }
    public int numClasses() { return classes(); }
    public int classOf()    { return _data.classOf(_index); }
    public final short getEncodedColumnValue(int colIndex) {
      return _data.getEncodedColumnValue(_index, colIndex);
    }
  }

  protected final DataAdapter _data;

  /** Returns new Data object that stores all adapter's rows unchanged.   */
  public static Data make(DataAdapter da) { return new Data(da); }

  protected Data(DataAdapter da) {
    _data = da;
    _columnInfo = new ColumnInfo[_data.columns()];
    for(int i = 0; i<_columnInfo.length; i++) {
      _columnInfo[i] = _data.ignore(i) ? null : new ColumnInfo(i);
    }
  }

  protected int start()          { return 0;                   }
  protected int end()            { return _data._numRows;      }
  public int badRows()           { return _data._badRows;      }
  public int rows()              { return end() - start();     }
  public int columns()           { return _data.columns();     }
  public int available_columns() { return _data.available_columns(); }
  public int classes()           { return _data.classes();     }
  public long seed()             { return _data.seed();        }
  public long dataId()           { return _data.dataId();      }
  public int classIdx()          { return _data._classIdx;     }
  public String colName(int i)   { return _data.columnNames(i); }
  public float unmap(int col, int split) { return _data.unmap(col, split); }
  public int columnArity(int colIndex) { return _data.columnArity(colIndex); }
  public boolean ignore(int col) { return _data.ignore(col);   }
  public boolean isFloat(int col){ return _data.isFloat(col); }
  public double[] classWt()      { return _data._classWt; }

  public final Iterator<Row> iterator() { return new RowIter(start(), end()); }
  private class RowIter implements Iterator<Row> {
    final Row _r = new Row();
    int _pos = 0; final int _end;
    public RowIter(int start, int end) { _pos = start; _end = end;       }
    public boolean hasNext()           { return _pos < _end;             }
    public Row next()                  { _r._index = permute(_pos++); return _r; }
    public void remove()               { throw new Error("Unsupported"); }
  }

  public void filter(SplitNode node, Data[] result, Statistic ls, Statistic rs) {
    final Row row = new Row();
    int[] permutation = getPermutationArray();
    int l = start(), r = end() - 1;

    while (l <= r) {
      int permIdx = row._index = permutation[l];
      if (node.isIn(row)) {
        ls.addQ(row);
        ++l;
      } else {
        rs.addQ(row);
        permutation[l] = permutation[r];
        permutation[r--] = permIdx;
      }
    }
    assert r+1 == l;
    ls.applyClassWeights();     // Weight the distributions
    rs.applyClassWeights();     // Weight the distributions
    ColumnInfo[] linfo = _columnInfo.clone();
    ColumnInfo[] rinfo = _columnInfo.clone();
    linfo[node._column]= linfo[node._column].left(node._split);
    rinfo[node._column]= rinfo[node._column].right(node._split);
    result[0]= new Subset(this, permutation, start(), l);
    result[1]= new Subset(this, permutation, l,   end());
    result[0]._columnInfo = linfo;
    result[1]._columnInfo = rinfo;
  }

  public Data sampleWithReplacement(double bagSizePct, short[] complement) {
    // Make sure that values come in order
    short[] in = complement;
    int size = (int)(rows() * bagSizePct);
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed());
    for( int i = 0; i < size; ++i)
      in[permute(r.nextInt(rows()))]++;
    int[] sample = MemoryManager.malloc4(size);
    for( int i = 0, j = 0; i < sample.length;) {
      while(in[j]==0) j++;
      for (int k = 0; k < in[j]; k++) sample[i++] = j;
      j++;
    }
    return new Subset(this, sample, 0, sample.length);
  }


  private int[] sample_resevoir(double bagSizePct, long seed, int numrows ) {
    // Resevoir Sampling.  First fill with sequential valid rows.
    // i ranges from 0 to rows() (all the data).
    // j    is the number of *valid* rows seen so far.
    // rows is the number of *valid* rows total.
    // invariant:  size/rows==bagSizePct
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed);
    int rows = rows();
    int size = bagsz(rows,bagSizePct);
    int[] sample = MemoryManager.malloc4(size);
    int i = 0, j = 0;
    for( ; j<size; i++ )                 // Until we get 'size' valid rows
      if( _data.classOf(i) == -1 )       // Invalid row?
        size = bagsz(--rows,bagSizePct); // Toss out from row-cnt & sample-size
      else sample[j++] = i;              // Keep valid row
    // Resample the rest.
    for( ; i < rows(); i++ ) {
      if( _data.classOf(i) == -1 ) {     // Invalid row?
        size = bagsz(--rows,bagSizePct); // Toss out from row-cnt & sample-size
      } else {                           // Valid row; sample it
        // Resevoir Sampling: pick the next value from 0 to #rows seen so far
        // but it is an INCLUSIVE pick, so pre-increment j.  Imagine the
        // degenerate case of size=1 - from 100 rows.  We want to pick a single
        // final row at random.  At this point sample[0]=0 and j=1 (1 valid
        // row).  r.nextInt(1) will always yield a 0... which forces this
        // die-roll to pick row 1 over row 0, and row 0 can never be picked.
        int p = r.nextInt(++j); // Roll a dice for all valid rows
        if( p < size ) sample[p] = i;
      }
    }
    return Arrays.copyOf(sample,size); // Trim out bad rows
  }

  // Roll a fair die for sampling, resetting the random die every numrows
  private int[] sample_fair(double bagSizePct, long seed, int numrows ) {
    Random r = null;
    int rows = rows();
    int size = bagsz(rows,bagSizePct);
    int[] sample = MemoryManager.malloc4((int)(size*1.10));
    float f = (float)bagSizePct;
    int cnt=0;                  // Counter for resetting Random
    int j=0;                    // Number of selected samples
    for( int i=0; i<rows(); i++ ) {
      if( cnt--==0 ) {
        /* NOTE: Before changing used generator think about which kind of random generator you need:
         * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
        r = Utils.getDeterRNG(seed+(i<<16)); // Seed is seed+(chunk#*numrows)
        cnt=numrows-1;          //
        if( i+2*numrows > rows() ) cnt = rows(); // Last chunk is big
      }
      if( _data.classOf(i) != -1 && r.nextFloat() < f ) {
        if( j == sample.length ) sample = Arrays.copyOfRange(sample,0,(int)(sample.length*1.2));
        sample[j++] = i;
      }
    }
    return Arrays.copyOf(sample,j); // Trim out bad rows
  }
  // added for stratified sampling, uniformly picks sample of n elements from the given interval
  private int sampleFromClass(int c, int n, int startIdx, int sample [], Random r) {
    int iStart = _data._intervalsStarts[c];
    int iEnd = _data._intervalsStarts[c+1];
    int iWidth = iEnd - iStart;
    for(int i = 0; i < n; ++i){
      int candidate = iStart + r.nextInt(iWidth);
      while(_data.classOf(candidate) == -1){
        if(candidate == iStart)candidate = iStart + iWidth;
        --candidate;
      }
      sample[startIdx++] = candidate;
    }
    return startIdx;
  }

  public Data sample(int [] strata, long seed) {
    int sz = 0;
    for(int s:strata)sz += s;
    int [] sample = new int[sz];
    int idx = 0;
    /* NOTE: Before changing used generator think about which kind of random generator you need:
     * if always deterministic or non-deterministic version - see hex.rf.Utils.get{Deter}RNG */
    Random r = Utils.getRNG(seed);
    for(int i = 0; i < strata.length; ++i){
      idx = sampleFromClass(i, strata[i], idx, sample,r);
    }
    Arrays.sort(sample); // we want an ordered sample
    return new Subset(this, sample, 0, sample.length);
  }

  // Deterministically sample the 'this' Data at the bagSizePct.  Toss out
  // invalid rows (as-if not sampled), but maintain the sampling rate.
  public Data sample(double bagSizePct, long seed, int numrows) {
    assert getClass()==Data.class; // No subclassing on this method
    int [] sample;
    sample = sample_fair(bagSizePct,seed,numrows);
    // add the remaining rows
    Arrays.sort(sample); // we want an ordered sample
    return new Subset(this, sample, 0, sample.length);
  }
  private int bagsz( int rows, double bagSizePct ) {
    int size = (int)(rows * bagSizePct);
    return (size>0 || rows==0) ? size : 1;
  }

  public Data complement(Data parent, short[] complement) { throw new Error("Only for subsets."); }
  @Override public Data clone() { return this; }
  protected int permute(int idx) { return idx; }
  protected int[] getPermutationArray() {
    int[] perm = MemoryManager.malloc4(rows());
    for( int i = 0; i < perm.length; ++i ) perm[i] = i;
    return perm;
  }

  public int colMinIdx(int i) { return _columnInfo[i].min; }
  public int colMaxIdx(int i) { return _columnInfo[i].max; }

  class ColumnInfo {
    private final int col;
    int min, max;
    ColumnInfo(int col_) { col=col_; max = _data.columnArity(col_) - 1; }
    ColumnInfo left(int idx) {
      ColumnInfo res = new ColumnInfo(col);
      res.max = idx < max ? idx : max;
      res.min = min;
      return res;
    }
    ColumnInfo right(int idx) {
      ColumnInfo res = new ColumnInfo(col);
      res.min = idx >= min ? (idx+1) : min;
      res.max = max;
      return res;
    }
    int min() { return min; }
    int max() { return max; }

    public String toString() { return  col +  "["+ min +","+ max + "]"; }
  }

  ColumnInfo[] _columnInfo;
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
    super(data._data);
    _start       = start;
    _end         = end;
    _permutation = permutation;
  }

  @Override public Data complement(Data parent, short[] complement) {
    int size= 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) size++;
    int[] p = MemoryManager.malloc4(size);
    int pos = 0;
    for(int i=0;i<complement.length; i++) if (complement[i]==0) p[pos++] = i;
    return new Subset(this, p, 0, p.length);
  }
}
