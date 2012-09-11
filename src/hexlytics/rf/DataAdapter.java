package hexlytics.rf;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

public class DataAdapter  {
  short[] data_;
  C[] c_;
  private HashMap<String, Integer> c2i_ = new HashMap<String,Integer>();
  private String name_="";
  int classIdx_;
  final int _data_id;           // Unique cookie identifying this dataset
  private boolean frozen_;
  private int numClasses_=-1;
  private String[] columnNames_;
  private String classColumnName_;
  public final long seed;
  public final Random random_;
  private static int SEED = 42;
  private static Random RAND = new Random(SEED);

  private static long getRandomSeed() { return RAND.nextLong(); }

  public DataAdapter(String name, Object[] columns, String classNm, int rows, int data_id, int numClasses) {
    long seed = getRandomSeed(); name_=name;
    c_ = new C[columns.length];
    columnNames_ = new String[columns.length];
    // Note that the number of classes is not generally known in a distributed
    // fashion, as any single JVM does not see all the data - so it needs to be
    // passed in here.
    numClasses_ = numClasses;
    classColumnName_ = classNm;
    for( int i=0; i<columns.length; i++ ) {
      String s=columns[i].toString();  columnNames_[i] = s; c_[i]= new C(s,rows); c2i_.put(s,i);
    }
    classIdx_ = c2i_.get(classNm);
    assert 0 <= classIdx_ && classIdx_ < 100;
    _data_id = data_id;
    if (classIdx_ != columns.length-1) throw new Error("The class must be the last column");
    this.seed = seed;
    random_ = new Random(seed);
  }

    public String name() { return name_; }
    public void shrinkWrap() { 
      freeze();
      short[][] vss = new short[c_.length][];
      for( int i=0; i<c_.length-1; i++ )
        vss[i] = c_[i].shrink(false); // Short-Encode the raw data
      vss[c_.length-1] = c_[c_.length-1].shrink(true); // Do not encode the classes
      data_ = new short[ c_.length * rows()];
      for(int i=0;i<c_.length;i++) {
        short[] vs = vss[i];
        for(int j=0;j<vs.length;j++) setS(j,i,vs[j]);
      }      
    }
    
    public void freeze() { frozen_=true; }
    public int features() { return (int)Math.sqrt(c_.length); }
    public int columns()        { return c_.length;} 
    public int rows()           { return c_.length == 0 ? 0 : c_[0].sz_; }
    public int classOf(int idx) { return getS(idx,classIdx_); }  // (int) c_[classIdx_].v_[idx]; }
    
    public int classes() {   
      if (!frozen_) throw new Error("Data set incomplete, freeze when done.");
      if (numClasses_==-1) {
        C c = c_[classIdx_];
        numClasses_= (int)(c.max_-c.min_)+1;
      }
      return numClasses_;
    }
    // By default binning is not supported
    public int columnClasses(int colIndex) {
      return c_[colIndex].smax_;
    }
    
    // by default binning is not supported
    public int getColumnClass(int rowIndex, int colIndex) {
      return getS(rowIndex, colIndex);
    }

    public  String colName(int c) { return c_[c].name_; }
    public  double colMin(int c)  { return c_[c].min_; }    
    public  double colMax(int c)  { return c_[c].max_; }
    public  double colTot(int c)  { return c_[c].tot_; }
    public String[] columnNames() { return columnNames_; }
    public String   classColumnName() { return classColumnName_; }
    public void addRow(double[] v) {
      if (frozen_) throw new Error("Frozen data set update");
      for(int i=0;i<v.length;i++)  c_[i].add(v[i]);
    } 
    short getS(int row, int col) { return data_[row * c_.length + col]; }
    void setS(int row, int col, short val) { data_[row * c_.length + col]= val; }    
    protected double getD(int col, int idx) { return c_[col].getD(idx); }
    static final DecimalFormat df = new  DecimalFormat ("0.##");
}

class C {
  String name_;
  int sz_;
  double min_=Double.MAX_VALUE, max_=Double.MIN_VALUE, tot_;
  double[] v_;
  HashMap<Double,Short> o2v_;
  double[] _v2o;  // Reverse (short) indices to original doubles
  short smin_ = -1;
  short smax_ = -1;

  C(String s, int rows) { name_ = s; v_ = new double[rows]; }

  void add(double x) {
    min_=Math.min(x,min_);
    max_=Math.max(x,max_);
    tot_+=x;
    v_[sz_++]=x;
  }
  
  double getD(int i) { return v_[i]; }
  
  public String toString() {
    String res = "col("+name_+")";
    res+= "  ["+DataAdapter.df.format(min_) +","+DataAdapter.df.format(max_)+"], avg=";
    res+= DataAdapter.df.format(tot_/(double)sz_) ;
    return res;
  }

  // For all columns except the classes - encode all doubles as unique shorts.
  // For the last column holding the classes - encode it as 0-(numclasses-1).
  // Sometimes the last column allows a zero class (e.g. iris, poke) and sometimes
  // it's one-based (e.g. covtype).
  short[] shrink( boolean noEncoding ) {
    smin_ = 0;
    o2v_ = hashCol();
    short[] res = new short[sz_];
    int min = (int)min_;
    for(int j=0;j<sz_;j++)
      res[j] = noEncoding ? (short)((int)v_[j]-min) : o2v_.get(v_[j]).shortValue();
    v_= null;
    return res;
  }
  
  HashMap<Double,Short> hashCol() {
    HashSet<Double> res = new HashSet<Double>();
    for(int i=0; i< sz_; i++) if (!res.contains(v_[i])) res.add(v_[i]);
    HashMap<Double,Short> res2 = new HashMap<Double,Short>(res.size());
    Double[] ks = res.toArray(new Double[res.size()]);
    _v2o = new double[ks.length];
    Arrays.sort(ks);      
    smax_ = 0;
    for( Double d : ks)  {
      _v2o[smax_] = d;
      res2.put(d, smax_++);
    }
    return res2;
  }
}
