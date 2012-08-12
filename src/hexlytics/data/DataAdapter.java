package hexlytics.data;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;


public class DataAdapter  {
    
    Col[] c_;
    private HashMap<String, Integer> c2i_ = new HashMap<String,Integer>();
    private String name_="";
    int classIdx_;
    private boolean frozen_;
    private int numClasses_=-1;
    private String[] columnNames_;
    private String classColumnName_;
    
    public final long seed;
    public final Random random_;
    
    private static int SEED = 42;
    private static Random RAND = new Random(SEED);    
    
    private static long getRandomSeed() {
      return RAND.nextLong();    
    }
    public DataAdapter(String name, Object[] columns, String classNm) {
      this(name,columns,classNm,getRandomSeed());
    }
    public DataAdapter(Object[] columns, String classNm) {
      this(columns,classNm,getRandomSeed());
    }
    
    public DataAdapter(String name, Object[] columns, String classNm,long seed) { this(columns,classNm,seed); name_=name; } 
    public DataAdapter(Object[] columns, String classNm,long seed){
      c_ = new Col[columns.length]; 
      columnNames_ = new String[columns.length];
      classColumnName_ = classNm;
      int i=0; for(Object o:columns) { 
        String s=o.toString();  columnNames_[i] = s; c_[i]=new Col.D(s); c2i_.put(s,i++); 
       }
      classIdx_ = c2i_.get(classNm);
      if (classIdx_ != columns.length-1) throw new Error("The class must be the last column");
      this.seed = seed;
      random_ = new Random(seed);
    }
    
    private DataAdapter(DataAdapter d_) {
      this.seed = d_.seed;
      random_ = new Random(seed);
      name_ = d_.name_; columnNames_ = d_.columnNames_; classColumnName_= d_.classColumnName_;      
      frozen_=true;
      numClasses_=d_.numClasses_;
      classIdx_ = d_.classIdx_;
      c_ = new Col[d_.c_.length];
      c2i_ = d_.c2i_;
      int i = 0;
      for(Col c :d_.c_){
        if (c instanceof Col.D) {
          Col.D cd = (Col.D) c;
          if(c.prec_==0) {
            if (cd.max_ <= Byte.MAX_VALUE && cd.min_ >= Byte.MIN_VALUE) {
              Col.B nc = new Col.B(cd);
              for(int j=0;j<cd.sz_;j++) nc.v_[j] = (byte) cd.v_[j];
              c_[i++] = nc;                          
            }else if (cd.max_ <= Byte.MAX_VALUE && cd.min_ >= Byte.MIN_VALUE) {
              Col.I nc = new Col.I(cd);
              for(int j=0;j<cd.sz_;j++) nc.v_[j] = (int) cd.v_[j];
              c_[i] = nc;            
            } else {
              Col.D nc = new Col.D(cd);
              for(int j=0;j<cd.sz_;j++) nc.v_[j] = cd.v_[j];
              c_[i++] = nc;
            }
          } else {
            Col.D cf = new Col.D(cd);
            for(int j=0;j<cd.sz_;j++) cf.v_[j] = cd.v_[j];
            c_[i++] = cf;
          }
        } else if (c instanceof Col.F){
          Col.F cd = (Col.F)c; 
          Col.F cf = new Col.F(cd);
          for(int j=0;j<cd.v_.length;j++) cf.v_[j] = cd.v_[j];
          c_[i++] = cf;
        } else if (c instanceof Col.B){
          Col.B cd = (Col.B)c; 
          Col.B cf = new Col.B(cd);
          for(int j=0;j<cd.v_.length;j++) cf.v_[j] =  cd.v_[j];
          c_[i++] = cf;
        }else if (c instanceof Col.I){
          Col.I cd = (Col.I)c; 
          Col.I cf = new Col.I(cd);
          for(int j=0;j<cd.v_.length;j++) cf.v_[j] = cd.v_[j];
          c_[i++] = cf;
        }
      }
    }
    
    public void add(String c,  double v) { c_[ c2i_.get(c).intValue()].add(v); }
    public void add(String c,  int v) { c_[ c2i_.get(c).intValue()].add(v); }
 
    public String name() { return name_; }
    public DataAdapter shrinkWrap() { DataAdapter d = new DataAdapter(this); d.name_ += "->shrinked"; return d;  }
    public void freeze() { frozen_=true; }
    public int features() { 
       int v =(int)(2.0*columns()/3.0);
       if (v==0 || v >= columns()-1) throw new Error("Should pick 2/3 of columns");    
       return v;
    }
    public int columns()        { return c_.length;} 
    public int rows()           { return c_.length == 0 ? 0 : c_[0].sz_; }
    public int classOf(int idx) { return c_[classIdx_].getI(idx); }
    public int classes() {         
        if (!frozen_) throw new Error("Data set incomplete, freeze when done.");
        if (numClasses_==-1) numClasses_= (int)c_[classIdx_].max_+1;
        return numClasses_;
    }

    public  String colName(int c) { return c_[c].name_; }
    public  double colMin(int c)  { return c_[c].min_; }    
    public  double colMax(int c)  { return c_[c].max_; }
    public  double colTot(int c)  { return c_[c].tot_; }
    public  int    colPre(int c)  { return c_[c].prec_; }    
    public String[] columnNames() { return columnNames_; }
    public String   classColumnName() { return classColumnName_; }
   
    public void addRow(double[] v) {
      if (frozen_) throw new Error("Frozen data set update");
      for(int i=0;i<v.length;i++)  c_[i].add(v[i]);
    }
    public void getRow(int c, double[] v) { 
      for(int i=0;i<v.length;i++) v[i] = c_[i].getD(c);
    }
    protected int getI(int col, int idx) { return c_[col].getI(idx); }
    protected double getD(int col, int idx) { return c_[col].getD(idx); }


    static final DecimalFormat df = new  DecimalFormat ("0.##");
}

abstract class Col {
  String name_;
  int DEFAULT = 10;
  int GROWTH = 2;
  int sz_;
  double min_=Double.MAX_VALUE, max_=-1, tot_; 
  int prec_; 
  int[] dist_;
  abstract void add(double v);
  abstract void add(int v);
  abstract double getD(int i);
  abstract int getI(int i);
   
  int[] distribution() {
    if(dist_!=null) return dist_;
    HashMap<Double,Integer> d = new HashMap<Double,Integer>();
    for(int i=0;i<sz_;i++){
      int cnt = 0; double v = getD(i);
      if (d.containsKey(v))  cnt = d.get(v);
      d.put(v, ++cnt);      
    }
    int[] dd = new int[d.size()];
    int i=0;for(Integer v: d.values()) dd[i++] = v.intValue();
    return dist_= dd;
  }

  public String toString() {
    String res = "col("+name_+")";
    res+= "  ["+DataAdapter.df.format(min_) +","+DataAdapter.df.format(max_)+"], avg=";
    res+= DataAdapter.df.format(tot_/(double)sz_) + " precision=" + prec_; 
    return res;
  }
  static class I extends Col{
    int[] v_;
    I(String s) { name_ = s; v_ = new int[DEFAULT]; }
    I(Col o) { sz_=o.sz_;name_ = o.name_; min_=o.min_; max_=o.max_;tot_=o.tot_;prec_=o.prec_;v_=new int[sz_+1]; }
    I(Col o, int from, int to) {
      assert (to-from)>= o.sz_; sz_=to - from;name_ = o.name_; prec_=o.prec_;v_=new int[to-from]; int j=0;
      for(int i=from;i<to;i++) {
        int x = ((Col.I)o).v_[i]; min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[j++] = x;
      }
    }
    void grow() { if (sz_==v_.length) v_=Arrays.copyOf(v_, v_.length*GROWTH); } 
    void add(int x) { grow(); min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[sz_++]=x; }
    void add(double x) { add((int)x); }
    double getD(int i) { return v_[i]; }
    int getI(int i) { return v_[i]; }
  }
  static class B extends Col{
    byte[] v_;
    B(String s) { name_ = s; v_ = new byte[DEFAULT]; }
    B(String s, int sz) { name_ = s; v_ = new byte[sz]; }
    B(Col o) {sz_=o.sz_; name_ = o.name_; min_=o.min_; max_=o.max_;tot_=o.tot_;prec_=o.prec_;v_=new byte[sz_+1]; }
    B(Col o, int from, int to) {
      assert (to-from)<= o.sz_; sz_=to - from;name_ = o.name_; prec_=o.prec_;v_=new byte[to-from]; int j=0;
      for(int i=from;i<to;i++) {
        byte x = ((Col.B)o).v_[i]; min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[j++] = x;
      }
    }
    void grow() { if (sz_==v_.length) v_=Arrays.copyOf(v_, v_.length*GROWTH); }
    void add(int x) {grow(); min_= Math.min(x,min_); max_=Math.max(x,max_); tot_+=x; v_[sz_++]=(byte)x;}
    void add(double x) { add((int)x); }
    double getD(int i) { return v_[i]; }
    int getI(int i) { return v_[i]; }
  }
  static class D extends Col{
    double[] v_;
    D(String s) { name_ = s; v_ = new double[DEFAULT]; }
    D(String s, int sz) { name_ = s; v_ = new double[sz]; }
    D(Col o){ sz_=o.sz_; name_=o.name_; min_=o.min_; max_=o.max_;tot_=o.tot_;prec_=o.prec_;v_=new double[sz_+1]; }
    D(Col o, int from, int to) {
      assert (to-from)>= o.sz_; sz_=to - from;name_ = o.name_; prec_=o.prec_;v_=new double[to-from]; int j=0;
      for(int i=from;i<to;i++) {
        double x = ((Col.D)o).v_[i]; min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[j++] = x;
      }
    }
    void grow() { if (sz_==v_.length) v_=Arrays.copyOf(v_, v_.length*GROWTH); } 
    void add(double x){grow(); min_=Math.min(x,min_); max_=Math.max(x,max_); tot_+=x; v_[sz_++]=x;
      if (x != (double) (int)x) {
       String s = String.valueOf(x); int off = s.lastIndexOf("."); // really dumb way to get digits. 
       if (off>0) prec_ = Math.max(prec_, s.length()-off-1); // easy speed up.
      }
    }
    void add(int x) { add( (double) x); }
    double getD(int i) { return v_[i]; }
    int getI(int i) { return (int) v_[i]; }
  }
  static class F extends Col{
    float[] v_;
    F(String s) { name_ = s; v_ = new float[DEFAULT]; }
    F(String s, int sz) { name_ = s; v_ = new float[sz]; }
    F(Col o){ sz_=o.sz_; name_=o.name_; min_=o.min_; max_=o.max_;tot_=o.tot_;prec_=o.prec_;v_=new float[sz_+1]; }
    F(Col o, int from, int to) {
      assert (to-from)>= o.sz_; sz_=to - from;name_ = o.name_; prec_=o.prec_;v_=new float[to-from]; int j=0;
      for(int i=from;i<to;i++) {
        float x = ((Col.F)o).v_[i]; min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[j++] = x;
      }
    }
    void grow() { if (sz_==v_.length) v_=Arrays.copyOf(v_, v_.length*GROWTH); } 
    void add(double x) { grow(); min_= Math.min(x, min_); max_= Math.max(x, max_); tot_+=x; v_[sz_++]=(float)x;
      if (x != (double) (int)x) {
        String s = String.valueOf(x); int off = s.lastIndexOf("."); // really dumb way to get digits. 
        if (off>0) prec_ = Math.max(prec_, s.length()-off-1); // easy speed up.
      }
    }
    void add(int x) { add((double) x); }
    double getD(int i) { return v_[i]; }
    int getI(int i) { return (int) v_[i]; }
  }
}
