package hex.rf;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.BitSet;

import water.*;

import com.google.common.primitives.Ints;

class DataAdapter  {
  private final int _numClasses;
  private final String[] _columnNames;
  private final C[] _c;
  public  final ValueArray _ary;
  /** Unique cookie identifying this dataset*/
  private final int _dataId;
  private final long _seed;
  public final int _classIdx;
  public final int _numRows;
  public final double[] _classWt;

  /** Maximum arity for a column (not a hard limit at this point) */
  final short _bin_limit;

  DataAdapter(ValueArray ary, int classCol, int[] ignores, int rows,
              int data_id, long seed, short bin_limit, double[] classWt) {
    _seed = seed+((long)data_id<<16);
    _ary = ary;
    _bin_limit = bin_limit;
    _columnNames = ary.col_names();
    _c = new C[_columnNames.length];

    _numClasses = (int)(ary.col_max(classCol) - ary.col_min(classCol))+1;
    assert 0 <= _numClasses && _numClasses < 65535;

    _classIdx = classCol;
    assert ignores.length < _columnNames.length - 1;
    for( int i = 0; i < _columnNames.length; i++ ) {
      boolean ignore = Ints.indexOf(ignores, i) >= 0;
      double range = ary.col_max(i) - ary.col_min(i);
      if( i==_classIdx ) range++; // Allow -1 as the invalid-row flag in the class
      if (range==0) { ignore = true; Utils.pln("Ignoring column " + i + " as all values are identical.");   }
      boolean raw = (ary.col_size(i) > 0 && ary.col_scale(i)==1.0 && range < _bin_limit && ary.col_max(i) >= 0); //TODO do it for negative columns as well
      C.ColType t = C.ColType.SHORT;
      if( raw && range <= 1 ) t = C.ColType.BOOL;
      else if( raw && range <= Byte.MAX_VALUE) t = C.ColType.BYTE;
      boolean do_bin = !raw && !ignore;
      _c[i]= new C(_columnNames[i], rows, i==_classIdx, t, do_bin, ignore,_bin_limit);
      if( raw ) {
        _c[i]._smax = (short)range;
        _c[i]._min = (float)ary.col_min(i);
        _c[i]._max = (float)ary.col_max(i);
      }
    }
    _dataId = data_id;
    _numRows = rows;
    assert classWt == null || classWt.length==_numClasses;
    _classWt = classWt;
  }

  /** Given a value in enum format, returns a value in the original range. */
  public float unmap(int col, int v){  // FIXME should this be a short???? JAN
    short idx = (short)v; // Convert split-point of the form X.5 to a (short)X
    C c = _c[col];
    if ( !c._bin ) return v + c._min;

    if (v == idx) {  // this value isn't a split
      return c._binned2raw[idx+0];
    } else {
      float flo = c._binned2raw[idx+0]; // Convert to the original values
      float fhi = (idx < _numRows) ? c._binned2raw[idx+1] : flo+1.0f;
      float fmid = (flo+fhi)/2.0f; // Compute an original split-value
      assert flo < fmid && fmid < fhi; // Assert that the float will properly split
      return fmid;
    }
  }

  public void computeBins(int col){_c[col].shrink();}

  public long seed()          { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return getEncodedColumnValue(idx,_classIdx); }
  public int dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** True if we should ignore column i. */
  public boolean ignore(int i){ return _c[i]._ignore; }

  /** Returns the number of bins, i.e. the number of distinct values in the
   * column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { return ignore(col) ? 0 : _c[col]._smax; }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int rowIndex, int colIndex) { return _c[colIndex].getValue(rowIndex);}

  /** Return the array of all column names including ignored and class. */
  public String[] columnNames() { return _columnNames; }

  public void addValueRaw(float v, int row, int col){
    _c[col].addRaw(v, row);
  }


  public void addValue(short v, int row, int col){
    _c[col].setValue(row,v);
  }

  public void addValue(float v, int row, int col){
    // Find the bin value by lookup in _bin2raw array which is sorted so we can do binary lookup.
    // The index returned is - length - 1 in case the value
    int idx = Arrays.binarySearch(_c[col]._binned2raw,v);
    if(idx < 0)idx = -idx - 1;
    if(idx >= _c[col]._smax)System.err.println("unexpected sv = " + idx);
    // The array lookup can return the length of the array in case the value
    // would be > max, which should (does) not happen right now, but just in
    // case for the future, cap it to the max bin value)
    _c[col].setValue(row, (short)Math.min(_c[col]._smax-1,idx));
  }

  // Mark this row as being invalid for tree-building, typically because it
  // contains invalid data in some columns.
  public void setBad(int row) {
    _c[_classIdx].setValue(row,(short)-1);
  }

  /** Should we bin this column? */
  public boolean binColumn(int col){ return _c[col]._bin; }

  private static class C {
    enum ColType {BOOL,BYTE,SHORT};
    ColType _ctype;
    String _name;
    public final boolean _ignore, _isClass, _bin;
    float _min=Float.MAX_VALUE, _max=Float.MIN_VALUE, _tot;
    short[] _binned;
    byte [] _bvalues;
    float[]  _raw;
    // TFloatIntHashMap _freq;
    float[] _binned2raw;
    BitSet _booleanValues;
    short _smax = -1;
    int _n;
    final short _bin_limit;
    static final DecimalFormat df = new  DecimalFormat ("0.##");

    C(String s, int rows, boolean isClass, ColType t, boolean bin, boolean ignore, short bin_limit) {
      _name = s;
      _isClass = isClass;
      _ignore = ignore;
      _bin = bin;
      _bin_limit = bin_limit;
      _ctype = t;
      _n = rows;
      if( ignore ) return;        // Ignore this column
      if( bin ) {
        _raw = MemoryManager.allocateMemoryFloat(rows);
        return;
      }
      switch( _ctype ) {
      case BOOL:  _booleanValues = new BitSet(rows);  break;
      case BYTE:  _bvalues = MemoryManager.allocateMemory(rows);  break;
      case SHORT: _binned  = MemoryManager.allocateMemoryShort(rows);  break;
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
        if(_bvalues == null)_bvalues = MemoryManager.allocateMemory(_n);
        _bvalues[row] = (byte)s;
        break;
      case SHORT:
        if(_binned == null)_binned = MemoryManager.allocateMemoryShort(_n);
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
      assert _bin;
      _min=Math.min(x,_min);
      _max=Math.max(x,_max);
      _tot+=x;
      if(_raw == null)_raw = MemoryManager.allocateMemoryFloat(_n);
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
      if( _ignore ) return;
      assert !_isClass;
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
      _binned2raw = MemoryManager.allocateMemoryFloat(Math.min(n, _bin_limit));
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
      _binned = MemoryManager.allocateMemoryShort(_n);
      _raw = null;
    }
  }
}
