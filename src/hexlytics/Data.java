package hexlytics;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public abstract class Data  implements Iterable<Integer>, Iterator<Integer> {
  
  private static long SEED_;
  public static Random RANDOM = new Random(42);
  
  public static Data make(String name, Object[] columns, String classNm) { return new DataImpl(name,columns,classNm); }
  public static void setSeed(long seed) { SEED_ = seed; RANDOM=new Random(SEED_); } 
  
  public final Random random_ = new Random(RANDOM.nextLong());
  
  public abstract void add(String c,  double v);
  public abstract  void add(String c,  int v);
  public abstract  void add(int c, double v);
  public abstract  void add(int  c, int v);
  public abstract Data shrinkWrap();
  public abstract void freeze();
  public abstract int features();
  public abstract int columns();
  public abstract int rows();
  public abstract String name();
  public abstract int classOf();
  public abstract int classes();
  public  Iterator<Integer> iterator() { return this; }
  public abstract boolean hasNext();
  public abstract Integer next();
  public  void remove() { throw new Error("Unsported"); }
  public abstract Data seek(int idx);
  public abstract Data select(int from, int to);
  public abstract void addRow(double[] v);
  public abstract void getRow(double[] v); 
  public abstract int getI(int col, int idx);
  public abstract double getD(int col, int idx);
  public abstract Data sampleWithReplacement(float bagSize);


  public static abstract class Wrap  extends Data {
    protected Data d_;
    public Wrap(Data d) { d_ = d; }
    public  void add(String c,  double v) {  d_.add(c, v);}
    public  void add(String c,  int v) {  d_.add(c, v); }
    public  void add(int c, double v) {  d_.add(c, v); }
    public  void add(int  c, int v) {  d_.add(c,v); }
    public  Data shrinkWrap() { return this; }
    public  void freeze() { }
    public  int features() { return d_.features(); }
    public  int columns() { return d_.columns(); }
    public  int rows() { return d_.rows(); }
    public  String name() { return d_.name(); }
    public  int classOf() { return d_.classOf(); }
    public  int classes() { return d_.classes(); }
    public  boolean hasNext() { return d_.hasNext(); }
    public  Integer next() { return d_.next(); }
    public  Data seek(int idx) { return d_.seek(idx);}
    public  Data select(int from, int to) { return d_.select(from, to);}
    public  void addRow(double[] v){  d_.addRow(v);   }
    public  void getRow(double[] v) {  d_.getRow(v); }
    public  int getI(int col, int idx) { return d_.getI(col, idx); }
    public  double getD(int col, int idx) { return d_.getD(col, idx); }
    public  Data sampleWithReplacement(float bagSize) { 
      Data dd =  d_.sampleWithReplacement(bagSize);
      return dd;
    }
  }
}


class DataImpl extends Data {
    
    private Col[] c_;
    private HashMap<String, Integer> c2i_ = new HashMap<String,Integer>();
    private int next_;
    private String name_="";
    private int classIdx_;
    private boolean frozen_;
    private int numClasses_=-1;
    
    public DataImpl(String name, Object[] columns, String classNm) { this(columns,classNm); name_=name; } 
    public DataImpl(Object[] columns, String classNm){
      c_ = new Col[columns.length];
      int i=0; for(Object o:columns) { String s=o.toString(); c_[i]=new Col.D(s); c2i_.put(s,i++); }
      classIdx_ = c2i_.get(classNm);
    }
    

    private DataImpl(DataImpl d_,int from, int to) {
      name_ = d_.name_;
      frozen_=true;
      numClasses_=d_.numClasses_;
      classIdx_ = d_.classIdx_;
      c_ = new Col[d_.c_.length];
      c2i_ = d_.c2i_;
      int i = 0;
      for(Col c :d_.c_){
        if (c instanceof Col.D) {
          Col.D cd = (Col.D) c;
          c_[i++] = new Col.D(cd, from, to);                          
        } else  if (c instanceof Col.F) {
          Col.F cd = (Col.F) c;
          c_[i++] = new Col.F(cd, from, to);                          
        } else  if (c instanceof Col.B) {
          Col.B cd = (Col.B) c;
          c_[i++] = new Col.B(cd, from, to);                          
        } else  if (c instanceof Col.I) {
          Col.I cd = (Col.I) c;
          c_[i++] = new Col.I(cd, from, to);                          
        } 
      }
    }
    
    private DataImpl(DataImpl d_) {
      name_ = d_.name_;
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
            // blindly go to float.
            Col.F cf = new Col.F(cd);
            for(int j=0;j<cd.sz_;j++) cf.v_[j] = (float) cd.v_[j];
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
    public void add(int c, double v) { c_[c].add(v); }
    public void add(int  c, int v) { c_[c].add(v); }
    public String name() { return name_; }
    public Data shrinkWrap() { DataImpl d = new DataImpl(this); d.name_ += "->shrinked"; return d;  }
    public void freeze() { frozen_=true; }
    public int features() { 
       int v =(int)(2.0*columns()/3.0);
       if (v==0 || v >= columns()-1) throw new Error("Should pick 2/3 of columns");    
       return v;
    }
    public int columns() { return c_.length; }
    public int rows() { return c_.length == 0 ? 0 : c_[0].sz_; }
    public int classOf() { return c_[classIdx_].getI(next_); }
    public int classes() {         
        if (!frozen_) throw new Error("Data set incomplete, freeze when done.");
        if (numClasses_==-1)
            numClasses_= c_[classIdx_].distribution().length;
        return numClasses_;
    }
    public Iterator<Integer> iterator() { return this; }
    public boolean hasNext() { return c_.length>0 && next_ > c_[0].sz_; } 
    public Integer next() { return next_++; }
    public void remove() { throw new Error("Unsported"); }
    public Data seek(int idx) { assert c_.length>0 && idx > c_[0].sz_;  next_ = idx; return this; }
    
    public Data select(int from, int to) { 
      DataImpl d = new DataImpl(this, from,to); d.name_+="->select("+from+","+to+")";      
      return d;
    }
    
    public Data sampleWithReplacement(float bagSize) {
      assert bagSize > 0 && bagSize <= 1.0;
      
      return new Sample(this); // NOT WORKING... :-)
    }
    
    static class Sample extends Data.Wrap {
      boolean[] occurrences;
      public Sample(Data d) {
        super(d);
      }
      
    }
    
    public void addRow(double[] v) {
      assert v.length==c_.length;
      if (frozen_) throw new Error("Frozen data set update");
      for(int i=0;i<v.length;i++)  c_[i].add(v[i]);
    }
    public void getRow(double[] v) { 
      assert v.length==c_.length;
      for(int i=0;i<v.length;i++) v[i] = c_[i].getD(next_);
    }
    public int getI(int col, int idx) { return c_[col].getI(idx); }
    public double getD(int col, int idx) { return c_[col].getD(idx); }
    public String toString() {
      String res = "Data "+ name_+"\n";
      if (c_.length>0) { res+= c_[0].sz_+" rows, " + c_.length + " cols, "+ classes() +" classes\n"; }
      String[][] s = new String[c_.length][4];
      int i=0;
      for(Col c: c_)  {
        s[i][0] = "col("+c.name_+")";
        s[i][1] = "["+df.format(c.min_) +","+df.format(c.max_)+"]" ;
        s[i][2] = " avg=" + df.format(c.tot_/(double)c.sz_);
        s[i][3] = " precision=" + c.prec_; 
        i++;
      }
      int[] l = new int[4];
      for(int j=0;j<4;j++) {
        for(int k=0;k<c_.length;k++) {
          l[j] = Math.max(l[j], s[k][j].length());
        }
      }
      for(int k=0;k<c_.length;k++) {
        for(int j=0;j<4;j++) { 
          res+= s[k][j];
          int pad = l[j] - s[k][j].length();
          for(int m=0;m<=pad;m++) res+=" ";
        }
        res+="\n";
      }
      res +="========\n";
      res +="class histogram\n";
      int[] dist = c_[classIdx_].distribution();
      int[] sorted = Arrays.copyOf(dist, dist.length);
      Arrays.sort(sorted);
      int max = sorted[sorted.length-1];
      int[] prop = new int[dist.length];
      for(int j=0;j<prop.length;j++)
        prop[j]= (int) (10.0 * ( (float)dist[j]/max )); 
      for(int m=10;m>=0;m--) {
        for(int j=0;j<prop.length;j++){
          if (prop[j]>= m) res += "**  ";
          else res+="    ";
        }
        res+="\n";
      }
      res+="[";
      for(int j=0;j<dist.length;j++) res+=dist[j]+((j==dist.length-1)?"":",");     
      res+="]\n";
      return res;
    }
    

    static final DecimalFormat df = new  DecimalFormat ("0.##");
}

abstract class Col {
  String name_;
  int DEFAULT = 10;
  int GROWTH = 2;
  int sz_;
  double min_, max_, tot_; 
  int prec_;
  abstract void add(double v);
  abstract void add(int v);
  abstract double getD(int i);
  abstract int getI(int i);
  
  int[] dist_;
  
  int[] distribution() {
    if(dist_!=null) return dist_;
    HashMap<Double,Integer> d = new HashMap<Double,Integer>();
    for(int i=0;i<sz_;i++){
      int cnt = 0; double v = getD(i);
      if (d.containsKey(v))  cnt = d.get(v);
      d.put(v, ++cnt);      
    }
    int[] dd = new int[d.size()];
    int i=0;
    for(Integer v: d.values()) 
      dd[i++] = v.intValue();
    return dist_= dd;
  }

  public String toString() {
    String res = "col("+name_+")";
    res+= "  ["+DataImpl.df.format(min_) +","+DataImpl.df.format(max_)+"], avg=" + DataImpl.df.format(tot_/(double)sz_) + " precision=" + prec_; 
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
      assert (to-from)>= o.sz_; sz_=to - from;name_ = o.name_; prec_=o.prec_;v_=new byte[to-from]; int j=0;
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

