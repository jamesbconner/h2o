package hex.rf;

import gnu.trove.map.hash.TFloatIntHashMap;
import gnu.trove.map.hash.TFloatShortHashMap;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;

class DataAdapter  {
  private short[] data_;
  private C[] c_;
  private HashMap<String, Integer> c2i_ = new HashMap<String,Integer>();
  private String name_="";
  public  int classIdx_;
  private final int _data_id;           // Unique cookie identifying this dataset
  private boolean frozen_;
  private int numClasses_=-1;
  private String[] columnNames_;
  private final int _seed;
  private int rows;

  /** Create a data adapter
   * @param name  of the dataset
   * @param columns names of the columns, can be empty in which case we use the column number
   * @param classNm  name of the column that has the predictor
   * @param rows number of rows
   * @param data_id  identifier of the chunk we are working on
   * @param seed  for pseudo random number generation
   * @param numClasses number of classes the data can belong to
   */
  DataAdapter(String name, Object[] columns, String classNm, int[] ignores, int rows, int data_id, int seed, int numClasses) {
    _seed = seed+data_id;
    name_=name;
    c_ = new C[columns.length];
    columnNames_ = new String[columns.length];
    // Note that the number of classes is not generally known in a distributed
    // fashion, as any single JVM does not see all the data - so it needs to be
    // passed in here.
    numClasses_ = numClasses;
    for( int i=0; i<columns.length; i++ ) {
      String s=columns[i].toString();  columnNames_[i] = s; c2i_.put(s,i);
    }
    classIdx_ = c2i_.get(classNm);
    assert ignores.length < columns.length;
    for( int i=0; i<columns.length; i++ ) {
      boolean ignore = false;
      for(int j=0;j<ignores.length;j++) if(ignores[j]==i) ignore=true;
      c_[i]= new C(columnNames_[i],rows, i==classIdx_,ignore);
    }
    _data_id = data_id;
  }

  /** Given a value in enum format, returns a value in the original range. */
  public float unmap(int col, float v){  // FIXME should this be a short???? JAN
    short idx = (short)v; // Convert split-point of the form X.5 to a (short)X
    C c = c_[col];
    if (v == idx) {  // this value isn't a split
      return c._v2o[idx+0];
    } else {
      float flo = c._v2o[idx+0]; // Convert to the original values
      float fhi = (idx < rows()) ? c._v2o[idx+1] : flo+1.0f;
      float fmid = (flo+fhi)/2.0f; // Compute an original split-value
      assert flo < fmid && fmid < fhi; // Assert that the float will properly split
      return fmid;
    }
  }
  /** Return the name of the data set. */
  public String name() { return name_; }

  /** Encode the data in a compact form.*/
  public void shrinkWrap() {
    freeze();
    // Note: currently we are allocating space for all columns, including the ones we ignore.
    // Changing this would reduce footprint.
    short[][] vss = new short[c_.length][];
    for( int i=0; i<c_.length; i++ )   vss[i] = c_[i].shrink();
    data_ = new short[ c_.length * rows()];
    for(int i=0;i<c_.length;i++) {
      if (ignore(i)) continue; short[] vs = vss[i]; for(int j=0;j<vs.length;j++) setS(j,i,vs[j]);
    }
  }

  public int  seed()          { return _seed; }
  public void freeze()        { frozen_=true; }
  public int columns()        { return c_.length;}
  public int rows()           { return rows; }
  public int classOf(int idx) { return getS(idx,classIdx_); }
  public int dataId()         { return _data_id; }
  /** The number of possible prediction classes. */
  public int classes()        { return numClasses_; }
  /** True if we should ignore column i. */
  public boolean ignore(int i)     { return c_[i].ignore(); }

  /** Returns the number of bins, i.e. the number of distinct values in the column.  Zero if we are ignoring the column. */
  public int columnArity(int col) { if (ignore(col)) return 0; else return c_[col].smax_; }
  /** Return a short that represents the binned value of the original row,column value.  */
  public short getEncodedColumnValue(int rowIndex, int colIndex) { return getS(rowIndex, colIndex); }

  /** Return the array of all column names including ignored and class. */
  public String[] columnNames() { return columnNames_; }

  /** Add a row to this data set. */
  public void addRow(float[] v) {
    if (frozen_) throw new Error("Frozen data set update");
    for(int i=0;i<v.length;i++)  c_[i].add(v[i]);
    rows++;
  }
  short getS(int row, int col) { return data_[row * c_.length + col]; }
  void setS(int row, int col, short val) { data_[row * c_.length + col]= val; }
  protected float getF(int col, int idx) { return c_[col].getF(idx); }
  static final DecimalFormat df = new  DecimalFormat ("0.##");

  private class C {
    String name_;
    boolean _ignore, _isClass;
    float min_=Float.MAX_VALUE, max_=Float.MIN_VALUE, tot_;
    float[] v_;

    float[] _v2o;  // Reverse (short) indices to original floats
    short smax_ = -1;

    C(String s, int rows, boolean isClass, boolean ignore) {
      name_ = s; v_ = new float[rows]; _isClass=isClass; _ignore=ignore;
    }

    void add(float x) {
      min_=Math.min(x,min_);
      max_=Math.max(x,max_);
      tot_+=x;
      v_[rows()]=x;
    }

    float getF(int i) { return v_[i]; }
    boolean ignore() { return _ignore; }


    public String toString() {
      String res = "col("+name_+")";
      if (ignore()) return res + " ignored!";
      res+= "  ["+DataAdapter.df.format(min_) +","+DataAdapter.df.format(max_)+"], avg=";
      res+= DataAdapter.df.format(tot_/rows()) ;
      if (_isClass) res+= " CLASS ";
      return res;
    }

    /** For all columns except the classes - encode all floats as unique shorts.
        For the column holding the classes - encode it as 0-(numclasses-1).
        Sometimes the class allows a zero class (e.g. iris, poker) and sometimes
        it's one-based (e.g. covtype) or -1/+1 (arcene).   */
    short[] shrink() {
      if (ignore()) return null;
      short[] res = new short[rows()];
      if( _isClass ) {
        for(float v : v_) smax_ = v > smax_ ? (short) v : smax_;
        int min = (int)min_;
        for( int j = 0; j < rows(); j++ ) res[j] = (short)((int)v_[j]-min);
      } else {
        TFloatShortHashMap o2v2 = hashCol();
        for( int j = 0; j < rows(); j++ ) res[j] = o2v2.get(v_[j]);
      }
      v_= null;
      return res;
    }

    /** Maximum arity for a column (not a hard limit at this point) */
    static final short BIN_LIMIT = 1024;

    TFloatShortHashMap hashCol() {
      // Remove duplicate floats
      TFloatIntHashMap freq = new TFloatIntHashMap(100);
      for( int i = 0; i < rows(); i++ ) freq.adjustOrPutValue(v_[i], 1, 1);

      // Compute bin-size
      int bin_size = (freq.size() > BIN_LIMIT) ? (rows() / BIN_LIMIT) : 1;
      if( bin_size > 1 )
        Utils.pln(this + " this column's arity was cut from "+ freq.size()+ " to " + BIN_LIMIT);

      float[] ks = freq.keys();
      Arrays.sort(ks);

      // Assign shorts to floats, with binning.
      _v2o = new float[ks.length]; // Reverse mapping
      TFloatShortHashMap res = new TFloatShortHashMap();
      smax_ = 0;
      int bin_cnt = 0;
      for( float d : ks ) {
        _v2o[smax_] = d;           // Reverse mapping
        res.put(d, smax_);         // Forward mapping
        bin_cnt += freq.get(d);    // Grow bin
        if( bin_cnt > bin_size ) { // This bin is full?
          smax_++;                 // Flip bins!
          bin_cnt=0;
        }
      }
      return res;
    }
  }
}
