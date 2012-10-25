package hex.rf;

import gnu.trove.map.hash.*;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;

import com.google.common.primitives.Ints;

import jsr166y.RecursiveAction;
import water.MemoryManager;
import water.ValueArray;

class DataAdapter  {
  private short[] _data;
  private final int _numClasses;
  private final String[] _columnNames;
  private final C[] _c;
  private final ValueArray _ary;
  private final int _dataId;           // Unique cookie identifying this dataset
  private final int _seed;
  public final int _classIdx;
  public final int _numRows;

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
      _c[i]= new C(_columnNames[i], rows, i==_classIdx, ignore);
    }
    _dataId = data_id;
    _numRows = rows;
  }

  /** Given a value in enum format, returns a value in the original range. */
  public float unmap(int col, float v){  // FIXME should this be a short???? JAN
    short idx = (short)v; // Convert split-point of the form X.5 to a (short)X
    C c = _c[col];
    if (v == idx) {  // this value isn't a split
      return c._v2o[idx+0];
    } else {
      float flo = c._v2o[idx+0]; // Convert to the original values
      float fhi = (idx < _numRows) ? c._v2o[idx+1] : flo+1.0f;
      float fmid = (flo+fhi)/2.0f; // Compute an original split-value
      assert flo < fmid && fmid < fhi; // Assert that the float will properly split
      return fmid;
    }
  }

  /** Return the name of the data set. */
  public String name() { return _ary._key.toString(); }

  /** Encode the data in a compact form.*/
  public ArrayList<RecursiveAction> shrinkWrap() {
    freeze();
    ArrayList<RecursiveAction> res = new ArrayList(_c.length);
    // Note: currently we are allocating space for all columns, including the ones we ignore.
    // Changing this would reduce footprint.
    for(int i=0;i<_c.length;i++) {
      if( ignore(i) ) continue;
      final int col = i;
      res.add(new RecursiveAction() {
        protected void compute() {
          short[] vs = _c[col].shrink();
          for(int j = 0; j < vs.length; j++) setS(j, col, vs[j]);
        };
      });
    }
    return res;
  }

  public void freeze()        { _data = MemoryManager.allocateMemoryShort(_c.length * _numRows); }
  public int seed()           { return _seed; }
  public int columns()        { return _c.length;}
  public int classOf(int idx) { return getS(idx,_classIdx); }
  public int dataId()         { return _dataId; }
  /** The number of possible prediction classes. */
  public int classes()        { return _numClasses; }
  /** True if we should ignore column i. */
  public boolean ignore(int i)     { return _c[i].ignore(); }

  /** Returns the number of bins, i.e. the number of distinct values in the column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { if (ignore(col)) return 0; else return _c[col]._smax; }
  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int rowIndex, int colIndex) { return getS(rowIndex, colIndex); }

  /** Return the array of all column names including ignored and class. */
  public String[] columnNames() { return _columnNames; }

  /** Add a row to this data set. */
  public void addRow(float[] v, int row) {
    if( _data != null ) throw new Error("Frozen data set update");
    for( int i = 0; i < v.length; i++ ) _c[i].add(v[i], row);
  }
  short getS(int row, int col) { return _data[row * _c.length + col]; }
  void setS(int row, int col, short val) { _data[row * _c.length + col]= val; }
  static final DecimalFormat df = new  DecimalFormat ("0.##");

  private static class C {
    String _name;
    boolean _ignore, _isClass;
    float _min=Float.MAX_VALUE, _max=Float.MIN_VALUE, _tot;
    float[] _v;
    float[] _v2o;  // Reverse (short) indices to original floats
    short _smax = -1;

    C(String s, int rows, boolean isClass, boolean ignore) {
      _name = s;
      _v = new float[rows];
      _isClass = isClass;
      _ignore = ignore;
    }

    void add(float x, int row) {
      _min=Math.min(x,_min);
      _max=Math.max(x,_max);
      _tot+=x;
      _v[row]=x;
    }

    boolean ignore() { return _ignore; }

    public String toString() {
      String res = "col("+_name+")";
      if (ignore()) return res + " ignored!";
      res+= "  ["+DataAdapter.df.format(_min) +","+DataAdapter.df.format(_max)+"], avg=";
      res+= DataAdapter.df.format(_tot/_v.length) ;
      if (_isClass) res+= " CLASS ";
      return res;
    }

    /** For all columns except the classes - encode all floats as unique shorts.
        For the column holding the classes - encode it as 0-(numclasses-1).
        Sometimes the class allows a zero class (e.g. iris, poker) and sometimes
        it's one-based (e.g. covtype) or -1/+1 (arcene).   */
    short[] shrink() {
      if (ignore()) return null;
      short[] res = new short[_v.length];
      if( _isClass ) {
        for(float v : _v) _smax = v > _smax ? (short) v : _smax;
        int min = (int)_min;
        for( int j = 0; j < _v.length; j++ ) res[j] = (short)((int)_v[j]-min);
      } else {
        TFloatShortHashMap o2v2 = hashCol();
        for( int j = 0; j < _v.length; j++ ) res[j] = o2v2.get(_v[j]);
      }
      _v= null;
      return res;
    }

    /** Maximum arity for a column (not a hard limit at this point) */
    static final short BIN_LIMIT = 1024;

    TFloatShortHashMap hashCol() {
      // Remove duplicate floats
      TFloatIntHashMap freq = new TFloatIntHashMap(100);
      for( int i = 0; i < _v.length; i++ ) freq.adjustOrPutValue(_v[i], 1, 1);

      // Compute bin-size
      int bin_size = (freq.size() > BIN_LIMIT) ? (_v.length / BIN_LIMIT) : 1;
      if( bin_size > 1 )
        Utils.pln(this + " this column's arity was cut from "+ freq.size()+ " to " + BIN_LIMIT);

      float[] ks = freq.keys();
      Arrays.sort(ks);

      // Assign shorts to floats, with binning.
      _v2o = new float[ks.length]; // Reverse mapping
      TFloatShortHashMap res = new TFloatShortHashMap();
      _smax = 0;
      int bin_cnt = 0;
      for( float d : ks ) {
        _v2o[_smax] = d;           // Reverse mapping
        res.put(d, _smax);         // Forward mapping
        bin_cnt += freq.get(d);    // Grow bin
        if( bin_cnt > bin_size ) { // This bin is full?
          _smax++;                 // Flip bins!
          bin_cnt=0;
        }
      }
      return res;
    }
  }
}
