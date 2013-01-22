package hex.rf;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;

import water.*;
import water.ValueArray.Column;

import com.google.common.primitives.Ints;
/**A DataAdapter maintains an encoding of the original data. Every raw value (of type float)
 * is represented by a short value. When the number of unique raw value is larger that binLimit,
 * the DataAdapter will perform binning on the data and use the same short encoded value to
 * represent several consecutive raw values.
 *
 * Missing values, NaNs and Infinity are treated as BAD data. */
final class DataAdapter  {
  /*Place holder for missing data, NaN, Inf in short encoding.*/
  static final short BAD = Short.MIN_VALUE;
  int _numClasses = -1;
  int [] _intervalsStarts;
  private final Col[] _c;
  public  final ValueArray _ary;
  /** Unique cookie identifying this dataset*/
  private final long _dataId;
  private final long _seed;
  public final int _classIdx;
  public final int _numRows;
  public final double[] _classWt;
  /** Maximum arity for a column (not a hard limit) */
  final short _bin_limit;
  /** Number of available columns */
  private final int _availableColumns;
  /** Number of bad rows */
  private int _ignoredRows;

  DataAdapter(ValueArray ary, int classCol, int[] ignores, int rows,
              long unique, long seed, short bin_limit, double[] classWt) {
    _seed = seed+(unique<<16); // This is important to preserve sampling selection!!!
    _ary = ary;
    _bin_limit = bin_limit;
    Column[] cols = ary._cols;
    _c = new Col[cols.length];
    _classIdx = classCol;
    assert ignores.length < cols.length - 1 : "Too many ignores";
    int availableColumns = 0;
    for( int i = 0; i < cols.length; i++ ) {
      boolean ignore = Ints.indexOf(ignores, i) >= 0;
      if (ignore) assert i != _classIdx : "Trying to ignore class feature";
      Column c = cols[i];
      double range = c._max - c._min;
      if( i==_classIdx) assert range != 0 : "All observations are of one class";
      if( i==_classIdx ) range++; // Allow -1 as the invalid-row flag in the class
      if (range==0) { ignore = true; Utils.pln("[DA] Ignoring column " + i + " as all values are identical.");   }
      _c[i]= new Col(c._name, rows, i==_classIdx,ignore,_bin_limit, c.isFloat());
      availableColumns += !_c[i].isIgnore() ? 1 : 0;
    }
    _dataId = unique;
    _numRows = rows;
    boolean trivial = true;
    for(double f: classWt) if (f != 1.0) trivial = false;
    _classWt = trivial ?  null : classWt;
    assert availableColumns <= cols.length - ignores.length : "Available columns are computed in wrong way!";
    assert availableColumns > 0 : "All columns are unusable";
    _availableColumns = availableColumns;
  }

  public void initIntervals(int n){
    _intervalsStarts = new int[n+1];
    _intervalsStarts[n] = _numRows;
  }
  public void setIntervalStart(int i, int S){
    if(_intervalsStarts == null)_intervalsStarts = new int[i+1];
    if(_intervalsStarts.length <= i)_intervalsStarts = Arrays.copyOf(_intervalsStarts, i+1);
    _intervalsStarts[i] = S;
  }

  /** Given a value in enum format, returns:  the value in the original format if no
   * binning was applied,  or if binning was applied a value that is inbetween
   * the idx and the next value.  If the idx is the last value return (2*idx+1)/2. */
  public float unmap(int col, int idx){ return _c[col].rawSplit(idx); }

  public void computeBins(int col){_c[col].shrink();}

  public boolean isFloat(int col) { return _c[col].isFloat(); }
  public long seed()          { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return _c[_classIdx].get(idx); }
  /**Returns true if the row has missing data. */
//  public boolean badRow(int idx) { return getEncodedColumnValue(idx,_classIdx) == -1; }
  public long dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** True if we should ignore column i. */
  public boolean ignore(int i){ return _c[i].isIgnore(); }
  /** Number of available columns (number of columns - number of ignored columns) */
  public final int availableColumns()  { return _availableColumns; }

  /** Returns the number of bins, i.e. the number of distinct values in the
   * column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { return _c[col].arity(); }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int row, int col) { return _c[col].get(row); }

  public void shrink() {
    for ( Col c: _c) c.shrink();
    _numClasses = _c[_classIdx].arity();
  }

  public String columnName(int i) { return _c[i].name(); }

  public boolean isValid(ValueArray va, AutoBuffer ab, int row, int col) {
    if (ignore(col)) return false;
    if (va.isNA(ab,row,col)) return false;
    if (!_c[col].isFloat()) return true;
    float f =(float) va.datad(ab,row,col);
    if (Float.isInfinite(f)) return false;
    return true;
  }

  public void add(float v, int row, int col){ _c[col].add(row,v); }

  public final void addBad(int row, int col) { _c[col].addBad(row); }

  public boolean hasBadValue(int row, int col) { return _c[col].isBad(row); }

  public boolean isBadRow(int row) { return _c[_classIdx].isBad(row); }

  public void markIgnoredRow(int row) {
    _c[_classIdx].addBad(row);
    _ignoredRows++;
  }

  private static class Col {
    /** Encoded values*/
    short[] binned;
    /** Original values, kept only during inhale*/
    float[] raw;
    /** Map from binned to original*/
    float[] binned2raw;
    boolean isClass, ignore, isFloat;
    int binLimit;
    String name;
    static final DecimalFormat df = new  DecimalFormat ("0.##");
    /** Total number of bad values in the column. */
    int invalidValues;
    float min, max;

    Col(String s, int rows, boolean isClass_, boolean ignore_, short binLimit_, boolean isFloat_) {
      name = s; isFloat = isFloat_; isClass = isClass_; ignore = ignore_; binLimit = binLimit_;
      if( ignore ) return;        // Ignore this column
      raw = MemoryManager.malloc4f(rows);
    }

    boolean isIgnore() { return ignore; }
    boolean isFloat() { return isFloat; }
    boolean isClass() { return isClass; }
    int arity() { return isIgnore()? 0 : binned2raw.length; }
    String name() { return name; }
    short get(int row) { return binned[row]; }
    void add(int row, float val) { raw[row] = val; }

    void addBad(int row) { raw[row] = Float.NaN; }

    private boolean isBadRaw(float f) { return Float.isNaN(f); }
    boolean isBad(int row) { return binned[row] == BAD; }

    /** For all columns - encode all floats as unique shorts. */
    void shrink() {
      if( ignore ) return;
      float[] vs = raw.clone();
      Arrays.sort(vs); // Sort puts all Float.NaN at the end of the array (according Float.NaN doc)
      int ndups = 0, i = 0, nans = 0; // Counter of all NaNs
      while(i < vs.length-1){      // count dups
        if (isBadRaw(vs[i+1]))  { nans = vs.length - i - 1; break; } // skip all NaNs
        int j = i+1;
        while(j < vs.length && vs[i] == vs[j]){  ++ndups; ++j; }
        i = j;
      }
      invalidValues = nans;
      assert vs.length > nans : "Nothing but missing values, should ignore column " + name;
      int n = vs.length - ndups - nans;
      int rem = n % binLimit;
      int maxBinSize = (n > binLimit) ? (n / binLimit + Math.min(rem,1)) : 1;
      // Assign shorts to floats, with binning.
      binned2raw = MemoryManager.malloc4f(Math.min(n, binLimit)); // if n is smaller than bin limit no need to compact
      int smax = 0, cntCurBin = 1;
      i = 0;
      binned2raw[0] = vs[i];
      for(; i < vs.length; ++i) {
        if(isBadRaw(vs[i])) break; // the first NaN, there are only NaN in the rest of vs[] array
        if(vs[i] == binned2raw[smax]) continue; // remove dups
        if( ++cntCurBin > maxBinSize ) {
          if(rem > 0 && --rem == 0)--maxBinSize; // check if we can reduce the bin size
          ++smax;
          cntCurBin = 1;
        }
        binned2raw[smax] = vs[i];
      }
      ++smax;
//      for(i = 0; i< vs.length; i++) if (!isBadRaw(vs[i])) break;
      // All Float.NaN are at the end of vs => min is stored in vs[0]
      min = vs[0];
      for(i = vs.length -1; i>= 0; i--) if (!isBadRaw(vs[i])) break;
      max = vs[i];
      vs = null; // GCed
      binned = MemoryManager.malloc2(raw.length);
      // Find the bin value by lookup in bin2raw array which is sorted so we can do binary lookup.
      for(i = 0; i < raw.length; i++)
        if (isBadRaw(raw[i]))
          binned[i] = BAD;
        else {
          short idx = (short) Arrays.binarySearch(binned2raw, raw[i]);
          if (idx >= 0) binned[i] = idx;
          else binned[i] = (short) (-idx - 1); // this occurs when we are looking for a binned value, we return the smaller value in the array.
          assert binned[i] < binned2raw.length;
        }
      if( n > binLimit )  Utils.pln(this+" this column's arity was cut from "+n+" to "+smax);
      raw = null; // GCced
    }

    /**Given an encoded short value, return the original float*/
    public float raw(int idx) { return binned2raw[idx]; }

    /**Given an encoded short value, return the float that splits that value with the next.*/
    public float rawSplit(int idx){
      if (idx == BAD) return Float.NaN;
      float flo = binned2raw[idx+0]; // Convert to the original values
      float fhi = (idx+1 < binned2raw.length)? binned2raw[idx+1] : flo+1.f;
      float fmid = (flo+fhi)/2.0f; // Compute a split-value
      assert flo < fmid && fmid < fhi : "Values " + flo +","+fhi ; // Assert that the float will properly split
      return fmid;
    }

    int rows() { return binned.length; }

    public String toString() {
      String res = "Column("+name+"){";
      if( ignore ) return res + " ignored!";
      res+= " ["+df.format(min) +","+df.format(max)+"]";
      res+=",bad values=" + invalidValues + "/" + rows();
      if (isClass) res+= " CLASS ";
      res += "}";
      return res;
    }
  }
}
