package hex.rf;

import gnu.trove.map.hash.TFloatIntHashMap;
import gnu.trove.map.hash.TFloatShortHashMap;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

import jsr166y.RecursiveAction;
import water.MemoryManager;
import water.ValueArray;

import com.google.common.primitives.Ints;

class DataAdapter  {
  private final int _numClasses;
  private final String[] _columnNames;
  private final C[] _c;
  private final ValueArray _ary;
  private final int _dataId;           // Unique cookie identifying this dataset
  private final int _seed;
  public final int _classIdx;
  public final int _numRows;

  //public static final int MAX_BIN_LOG = 2;
  /** Maximum arity for a column (not a hard limit at this point) */
  static final short BIN_LIMIT = 1024;

  DataAdapter(ValueArray ary, int classCol, int[] ignores, int rows,
      int data_id, int seed) {
    _seed = seed+data_id;
    _ary = ary;
    _columnNames = ary.col_names();
    _c = new C[_columnNames.length];

    _numClasses = (int)(ary.col_max(classCol) - ary.col_min(classCol))+1;
    assert 0 <= _numClasses && _numClasses < 65535;

    _classIdx = classCol;
    assert ignores.length < _columnNames.length;
    for( int i = 0; i < _columnNames.length; i++ ) {
      boolean ignore = Ints.indexOf(ignores, i) > 0;
      double range = _ary.col_max(i) - _ary.col_min(i);
      boolean raw = (_ary.col_size(i) > 0 && range < BIN_LIMIT && _ary.col_max(i) >= 0); //TODO do it for negative columns as well
      _c[i]= new C(_columnNames[i], rows, i==_classIdx, !raw, ignore);
      if(raw){
        _c[i]._smax = (short)range;
        _c[i]._min = (float)_ary.col_min(i);
        _c[i]._max = (float)_ary.col_max(i);
      }
    }
    _dataId = data_id;
    _numRows = rows;
  }

  /** Given a value in enum format, returns a value in the original range. */
  public float unmap(int col, float v){  // FIXME should this be a short???? JAN
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

  /** Return the name of the data set. */
  public String name() { return _ary._key.toString(); }

  /** Encode the data in a compact form.*/
  public ArrayList<RecursiveAction> shrinkWrap() {
    ArrayList<RecursiveAction> res = new ArrayList(_c.length);
    for( final C c : _c ) {
      if( c.ignore() || !c._bin) continue;
      res.add(new RecursiveAction() {
        protected void compute() {
          c.shrink();
        };
      });
    }
    return res;
  }

  public int seed()           { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return getS(idx,_classIdx); }
  public int dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** True if we should ignore column i. */
  public boolean ignore(int i)     { return _c[i].ignore(); }

  /** Returns the number of bins, i.e. the number of distinct values in the column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { return ignore(col) ? 0 : _c[col]._smax; }

  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int rowIndex, int colIndex) { return getS(rowIndex, colIndex); }

  /** Return the array of all column names including ignored and class. */
  public String[] columnNames() { return _columnNames; }

  public void addValueRaw(float v, int row, int col){
    _c[col].add(v, row);
  }

  public void addValue(short v, int row, int col){
    _c[col]._binned[row] = v;
  }
  /** Add a row to this data set. */
  public void addRow(float[] v, int row) {
    for( int i = 0; i < v.length; i++ ) _c[i].add(v[i], row);
  }
  short getS(int row, int col) { return _c[col]._binned[row]; }
  static final DecimalFormat df = new  DecimalFormat ("0.##");

  public boolean binColumn(int col){
    return _c[col]._bin;
  }

  private static class C {
    String _name;
    boolean _ignore, _isClass, _bin;
    float _min=Float.MAX_VALUE, _max=Float.MIN_VALUE, _tot;
    short[] _binned;
    float[] _raw;
    float[] _binned2raw;
    short _smax = -1;

    C(String s, int rows, boolean isClass, boolean bin, boolean ignore) {
      _name = s;
      _isClass = isClass;
      _ignore = ignore;
      _bin = bin;
      if(!_ignore){
        _raw = _bin?MemoryManager.allocateMemoryFloat(rows):null;
        _binned = _bin?null:MemoryManager.allocateMemoryShort(rows);
      }
    }

    void add(float x, int row) {
      assert _bin;
      _min=Math.min(x,_min);
      _max=Math.max(x,_max);
      _tot+=x;
      _raw[row]=x;
    }

    boolean ignore() { return _ignore; }

    public String toString() {
      String res = "col("+_name+")";
      if (ignore()) return res + " ignored!";
      res+= "  ["+DataAdapter.df.format(_min) +","+DataAdapter.df.format(_max)+"], avg=";
      res+= DataAdapter.df.format(_tot/_raw.length) ;
      if (_isClass) res+= " CLASS ";
      return res;
    }

    /** For all columns except the classes - encode all floats as unique shorts.
     *  For the column holding the classes - encode it as 0-(numclasses-1).
     *  Sometimes the class allows a zero class (e.g. iris, poker) and sometimes
     *  it's one-based (e.g. covtype) or -1/+1 (arcene).   */
    void shrink() {
      if (ignore()) return;
      _binned = MemoryManager.allocateMemoryShort(_raw.length);
      if( _isClass ) {
        _smax = (short) _max;
        int min = (int)_min;
        for( int j = 0; j < _raw.length; j++ )
          _binned[j] = (short)((int)_raw[j]-min);
      } else {
        // Remove duplicate floats
        TFloatIntHashMap freq = new TFloatIntHashMap(BIN_LIMIT);
        for( float f : _raw ) freq.adjustOrPutValue(f, 1, 1);

        // Compute bin-size
        int maxBinSize = (freq.size() > BIN_LIMIT) ? (_raw.length / BIN_LIMIT) : 1;
        if( maxBinSize > 1 )
          Utils.pln(this + " this column's arity was cut from "+ freq.size()+ " to " + BIN_LIMIT);

        float[] ks = freq.keys();
        Arrays.sort(ks);

        // Assign shorts to floats, with binning.
        _binned2raw = new float[Math.min(freq.size(), BIN_LIMIT)];
        TFloatShortHashMap o2v2 = new TFloatShortHashMap(ks.length);
        _smax = 0;
        int cntCurBin = 0;
        for( float d : ks ) {
          _binned2raw[_smax] = d;
          o2v2.put(d, _smax);
          cntCurBin += freq.get(d);
          if( cntCurBin > maxBinSize ) {
            ++_smax;
            cntCurBin = 0;
          }
        }
        for( int j = 0; j < _raw.length; j++ ) _binned[j] = o2v2.get(_raw[j]);
      }
      _raw = null;
    }


  }
}
