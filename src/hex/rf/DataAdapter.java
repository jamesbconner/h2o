package hex.rf;

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

  DataAdapter(String name, Object[] columns, String classNm, int rows, int data_id, int seed, int numClasses) {
    _seed = seed+data_id;
    name_=name;
    c_ = new C[columns.length];
    columnNames_ = new String[columns.length];
    // Note that the number of classes is not generally known in a distributed
    // fashion, as any single JVM does not see all the data - so it needs to be
    // passed in here.
    numClasses_ = numClasses;
    for( int i=0; i<columns.length; i++ ) {
      String s=columns[i].toString();  columnNames_[i] = s; c_[i]= new C(s,rows); c2i_.put(s,i);
    }
    classIdx_ = c2i_.get(classNm);
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

  public String name() { return name_; }

  // lame attempt at best effort ... throw away half the data each time 'round
  public void shrinkWrap() {
    freeze();
    short[][] vss = new short[c_.length][];
    for( int i=0; i<c_.length; i++ )
      vss[i] = c_[i].shrink(i==classIdx_); // Short-Encode the raw data, but not the class
    data_ = new short[ c_.length * rows()];
    for(int i=0;i<c_.length;i++) {
      short[] vs = vss[i];
      for(int j=0;j<vs.length;j++) setS(j,i,vs[j]);
    }
  }

  public int  seed()          { return _seed; }
  public void freeze()        { frozen_=true; }
  public int columns()        { return c_.length;}
  public int rows()           { return rows; }
  public int classOf(int idx) { return getS(idx,classIdx_); }  // (int) c_[classIdx_].v_[idx]; }
  public int dataId()         { return _data_id; }

  public int classes() {
    if (!frozen_) throw new Error("Data set incomplete, freeze when done.");
    if (numClasses_==-1) {
      C c = c_[classIdx_];
      numClasses_= (int)(c.max_-c.min_)+1;
    }
    return numClasses_;
  }
  // By default binning is not supported
  public int columnArity(int colIndex) {
    return c_[colIndex].smax_;
  }

  // by default binning is not supported
  public short getEncodedColumnValue(int rowIndex, int colIndex) {
    return getS(rowIndex, colIndex);
  }

  public String[] columnNames() { return columnNames_; }
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
    boolean ignore;
    float min_=Float.MAX_VALUE, max_=Float.MIN_VALUE, tot_;
    float[] v_;

    float[] _v2o;  // Reverse (short) indices to original floats
    short smax_ = -1;

    C(String s, int rows) { name_ = s; v_ = new float[rows]; }

    void add(float x) {
      min_=Math.min(x,min_);
      max_=Math.max(x,max_);
      tot_+=x;
      v_[rows()]=x;
    }

    float getF(int i) { return v_[i]; }
    void ignore() { ignore = true; }

    public String toString() {
      String res = "col("+name_+")";
      res+= "  ["+DataAdapter.df.format(min_) +","+DataAdapter.df.format(max_)+"], avg=";
      res+= DataAdapter.df.format(tot_/rows()) ;
      return res;
    }

    // For all columns except the classes - encode all floats as unique shorts.
    // For the last column holding the classes - encode it as 0-(numclasses-1).
    // Sometimes the last column allows a zero class (e.g. iris, poker) and sometimes
    // it's one-based (e.g. covtype) or -1/+1 (arcene)
    short[] shrink( boolean noEncoding ) {
      HashMap<Float,Short> o2v2 =//noEncoding? null :
          hashCol();
     // if (noEncoding) for(float v : v_) smax_ = v > smax_ ? (short) v : smax_;
      short[] res = new short[rows()];
      int min = (int)min_;
      for(int j=0;j<rows();j++)
        res[j] = noEncoding ? (short)((int)v_[j]-min) :  o2v2.get(v_[j]);
      v_= null;
      return res;
    }

    /** Maximum arity for a column (not a hard limit at this point) */
    static final short BIN_LIMIT = 1024;

    HashMap hashCol() {
      // Remove duplicate floats
      HashMap<Float,Integer> res = new HashMap<Float,Integer>(100);
      for( int i=0; i< rows(); i++ )
        res.put(v_[i], (res.containsKey(v_[i]) ? res.get(v_[i]) : 0) + 1);

      // Compute bin-size
      int bin_size = (res.size() > BIN_LIMIT) ? (rows() / BIN_LIMIT) : 1;
      if( bin_size > 1 )
        Utils.pln(this + " this column's arity was cut from "+ res.size()+ " to " + BIN_LIMIT);

      // Convert Floats to floats and sort
      Float[] fks = res.keySet().toArray(new Float[0]);
      float[] ks = new float[fks.length];
      for( int idx=0; idx<fks.length; idx++ )  ks[idx] = fks[idx];
      Arrays.sort(ks);

      // Assign shorts to floats, with binning.
      _v2o = new float[ks.length]; // Reverse mapping
      HashMap<Float,Short> res2 = new HashMap<Float,Short>(); // Forward mapping
      smax_ = 0;
      int bin_cnt = 0;
      for( float d : ks ) {
        _v2o[smax_] = d;          // Reverse mapping
        res2.put(d, smax_);       // Forward mapping
        bin_cnt += res.get(d);    // Grow bin
        if( bin_cnt > bin_size ) {// This bin is full?
          smax_++;                // Flip bins!
          bin_cnt=0;
        }
      }
      return res2;
    }
  }
}
