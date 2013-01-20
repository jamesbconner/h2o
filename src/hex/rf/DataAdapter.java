  package hex.rf;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;

import water.*;
import water.ValueArray.Column;

import com.google.common.primitives.Ints;

final class DataAdapter  {
  private final int _numClasses;
  int [] _intervalsStarts;
  int _badRows;
  private final C[] _c;
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
  private final int _available_columns;

  DataAdapter(ValueArray ary, int classCol, int[] ignores, int rows,
              long unique, long seed, short bin_limit, double[] classWt) {
    _seed = seed+(unique<<16); // This is important to preserve sampling selection!!!
    _ary = ary;
    _bin_limit = bin_limit;
    Column[] cols = ary._cols;
    _c = new C[cols.length];

    _numClasses = (int)(cols[classCol]._max - cols[classCol]._min)+1;
    assert 0 <= _numClasses && _numClasses < 65535;

    _classIdx = classCol;
    assert ignores.length < cols.length - 1;
    int available_columns = 0;
    for( int i = 0; i < cols.length; i++ ) {
      boolean ignore = Ints.indexOf(ignores, i) >= 0;
      Column c = cols[i];
      double range = c._max - c._min;
      if( i==_classIdx ) range++; // Allow -1 as the invalid-row flag in the class
      if (range==0) { ignore = true; Utils.pln("[DA] Ignoring column " + i + " as all values are identical.");   }
      boolean raw = (c._size > 0 && !c.isScaled() && range < _bin_limit && c._max >= 0); //TODO do it for negative columns as well
      raw = (i==_classIdx);
      C.ColType t = C.ColType.SHORT;
      if( raw && range <= 1 ) t = C.ColType.BOOL;
      else if( raw && range <= Byte.MAX_VALUE) t = C.ColType.BYTE;
      boolean do_bin = !raw && !ignore;
      _c[i]= new C(c._name, rows, i==_classIdx, t, do_bin, ignore,_bin_limit, c._scale>1);
      available_columns += !_c[i]._ignore ? 1 : 0;
      if( raw ) {
        _c[i]._smax = (short)range;
        _c[i]._min = (float)c._min;
        _c[i]._max = (float)c._max;
      }
    }
    _dataId = unique;
    _numRows = rows;
    assert classWt == null || classWt.length==_numClasses;
    _classWt = classWt;
    assert available_columns <= cols.length - ignores.length : "Avaiable columns are computed in wrong way!";
    _available_columns = available_columns;
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
  public float unmap(int col, int idx){
    C c = _c[col];
    if ( !c._bin ) return idx + c._min;

    assert idx < c._binned2raw.length : "Trying to reference binned value out of binned2raw array!";
    float flo = c._binned2raw[idx+0]; // Convert to the original values
    float fhi = c._binned2raw[idx+1];
    float fmid = (flo+fhi)/2.0f; // Compute a split-value
    assert flo < fmid && fmid < fhi; // Assert that the float will properly split
    return fmid;
  }

  public void computeBins(int col){_c[col].shrink();}
  public boolean isFloat(int col) { return _c[col]._isFloat; }
  public long seed()          { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return getEncodedColumnValue(idx,_classIdx); }
  public long dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** True if we should ignore column i. */
  public boolean ignore(int i){ return _c[i]._ignore; }
  /** Number of available columns (number of columns - number of ignored columns) */
  public final int available_columns()  { return _available_columns; }

  /** Returns the number of bins, i.e. the number of distinct values in the
   * column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { return ignore(col) ? 0 : _c[col]._smax; }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int rowIndex, int colIndex) { return _c[colIndex].getValue(rowIndex);}

  /** Return the array of all column names including ignored and class. */
  public String columnNames(int i) { return _c[i]._name; }

  public void addValueRaw(float v, int row, int col){ _c[col].addRaw(v, row); }

  public void addValue(short v, int row, int col){ _c[col].setValue(row,v); }

  public void addValue(float v, int row, int col){
    // Find the bin value by lookup in _bin2raw array which is sorted so we can do binary lookup.
    // The index returned is - length - 1 in case the value
    int idx = Arrays.binarySearch(_c[col]._binned2raw,v);
    if(idx < 0)idx = -idx - 1;
    if(idx >= _c[col]._smax)
      System.err.println("[DA] unexpected sv = " + idx);
    // The array lookup can return the length of the array in case the value
    // would be > max, which should (does) not happen right now, but just in
    // case for the future, cap it to the max bin value)
    _c[col].setValue(row, (short)Math.min(_c[col]._smax-1,idx));
  }

  /** Mark this row as being invalid for tree-building, typically because it
     contains invalid data in some columns.*/
  public void setBad(int row) {
    if(_c[_classIdx].getValue(row) != -1){ ++_badRows; _c[_classIdx].setValue(row,(short)-1); }
  }

  /** Should we bin this column? */
  public boolean binColumn(int col){ return _c[col]._bin; }

  private static class C {
    enum ColType {BOOL,BYTE,SHORT};
    ColType _ctype;
    String _name;
    public final boolean _ignore, _isClass, _bin, _isFloat;
    float _min=Float.MAX_VALUE, _max=Float.MIN_VALUE, _tot;
    short[] _binned;
    byte [] _bvalues;
    float[]  _raw;
    float[] _binned2raw;
    BitSet _booleanValues;
    short _smax = -1;
    int _n;
    final short _bin_limit;
    static final DecimalFormat df = new  DecimalFormat ("0.##");

    C(String s, int rows, boolean isClass, ColType t, boolean bin, boolean ignore, short bin_limit, boolean isFloat) {
      _name = s;
      _isFloat = isFloat;
      _isClass = isClass;
      _ignore = ignore;
      _bin = bin;
      _bin_limit = bin_limit;
      _ctype = t;
      _n = rows;
      if( ignore ) return;        // Ignore this column
      if( bin ) {
        _raw = MemoryManager.malloc4f(rows);
        return;
      }
      switch( _ctype ) {
      case BOOL:  _booleanValues = new BitSet(rows);  break;
      case BYTE:  _bvalues = MemoryManager.malloc1(rows);  break;
      case SHORT: _binned  = MemoryManager.malloc2(rows);  break;
      default: throw H2O.unimpl();
      }
    }

    public void setValue(int row, short s){
      switch(_ctype){
      case BOOL:
        if(_booleanValues == null) _booleanValues = new BitSet(_n);
        if(s == 1)_booleanValues.set(row);
        break;
      case BYTE:
        assert (byte)s == s : "(byte)"+s+" name="+_name+" _min="+_min+" _max"+_max;
        if(_bvalues == null)_bvalues = MemoryManager.malloc1(_n);
        _bvalues[row] = (byte)s;
        break;
      case SHORT:
        if(_binned == null)_binned = MemoryManager.malloc2(_n);
        _binned[row] = s;
      }
    }

    public short getValue(int i) {
      switch(_ctype){
      case BOOL:  return (short)(_booleanValues.get(i)?1:0);
      case BYTE:  return _bvalues[i];
      case SHORT: return _binned[i];
      }
      throw new Error("illegal column type " + _ctype);
    }

    void addRaw(float x, int row) {
      _min=Math.min(x,_min);
      _max=Math.max(x,_max);
      _tot+=x;
      if(_raw == null)_raw = MemoryManager.malloc4f(_n);
      _raw[row] = x;
    }

    public String toString() {
      String res = "Column("+_name+")";
      if( _ignore ) return res + " ignored!";
      res+= "  ["+df.format(_min) +","+df.format(_max)+"], avg=";
      res+= df.format(_tot/_n) ;
      if (_isClass) res+= " CLASS ";
      return res;
    }

    /** For all columns except the classes - encode all floats as unique shorts.
     *  For the column holding the classes - encode it as 0-(numclasses-1).
     *  Sometimes the class allows a zero class (e.g. iris, poker) and sometimes
     *  it's one-based (e.g. covtype) or -1/+1 (arcene).   */
    void shrink() {
      if( _ignore || _isClass ) return;
      Arrays.sort(_raw);
      int ndups = 0;
      int i = 0;
      // count dups
      while(i < _raw.length-1){
        int j = i+1;
        while(j < _raw.length && _raw[i] == _raw[j]){
          ++ndups;
          ++j;
        }
        i = j;
      }
      int n = _raw.length - ndups;
      int rem = n % _bin_limit;
      int maxBinSize = (n > _bin_limit) ? (n / _bin_limit + Math.min(rem,1)) : 1;
      // Assign shorts to floats, with binning.
      _binned2raw = MemoryManager.malloc4f(Math.min(n, _bin_limit));
      _smax = 0;
      int cntCurBin = 1;
      _binned2raw[0] = _raw[0];
      for(i = 1; i < _raw.length; ++i) {
        if(_raw[i] == _binned2raw[_smax])continue; // remove dups
        if( ++cntCurBin > maxBinSize ) {
          if(rem > 0 && --rem == 0)--maxBinSize; // check if we can reduce the bin size
          ++_smax;
          cntCurBin = 1;
        }
        _binned2raw[_smax] = _raw[i];
      }
      ++_smax;
      if( n > _bin_limit )
        Utils.pln(this + " this column's arity was cut from "+ n + " to " + _smax);
      _binned = MemoryManager.malloc2(_n);
      _raw = null;
    }
  }
}
