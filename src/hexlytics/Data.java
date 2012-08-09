package hexlytics;

import hexlytics.Data.Int;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
/**
 * The classes that mediate access to data. In the long run, we want these to support 
 * distributed data and lazy computation. We are mostly looking at getting the interface
 * right.
 * @author jan
 */
public abstract class Data  implements Iterable<Int>, Iterator<Int> {
  public static class Int { int _; private Int(int v_) { _=v_; } }
  public final Int next = new Int(0);
  
  static long SEED_;
  public static Random RANDOM = new Random(42);
  static final DecimalFormat df = new  DecimalFormat ("0.##");

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
  public abstract int classOf(int idx);
  public abstract int classes();
  public  Iterator<Int> iterator() { next._ = -1; return this; }
  public abstract boolean hasNext();
  public abstract Int next();
  public  void remove() { throw new Error("Unsported"); }
  public abstract Data seek(int idx);
  public abstract Data select(int from, int to);
  public abstract void addRow(double[] v);
  public abstract void getRow(double[] v); 
  public abstract void getRow(int col,double[] v); 
  protected abstract int getI(int col, int idx);
  protected abstract double getD(int col, int idx);
  public abstract int getI(int col);
  public abstract double getD(int col);
  public float weight() { return 1; } 
  public abstract String colName(int c);
  public abstract double colMin(int c);
  public abstract double colMax(int c);
  public abstract double colTot(int c);
  public abstract int colPre(int c);
  
  public abstract String[] columnNames();
  public abstract String   classColumnName();
  
  public Data view() { return new View(this); }
  public Data complement() { throw new Error("Unsupported"); }
  
  public Data sampleWithReplacement(double bagSize) {
    assert bagSize > 0 && bagSize <= 1.0;     
    return new Sample(this,bagSize); 
  }
  
  public Data sort(int column) { return new Shuffle(this.materialize(),column); }
  public Data filter(Classifier c, int direction) { return new Filter(this,c,direction); }
  
  public Data materialize() {
    if (this instanceof DataImpl) return this;
    DataImpl res = new DataImpl(name(), columnNames(), classColumnName());
    double[] v = new double[columns()];
    seek(0);
    for (Int it : this) {
      getRow(v);
      res.addRow(v);
    }
    res.freeze();
    return res;
  }

  public String toString() {
    String res = "Data "+ name()+"\n";
    if (columns()>0) { res+= rows()+" rows, "+ columns() + " cols, "+ classes() +" classes\n"; }
    String[][] s = new String[columns()][4];
    for(int i=0;i<columns();i++){
      s[i][0] = "col("+colName(i)+")";
      s[i][1] = "["+df.format(colMin(i)) +","+df.format(colMax(i))+"]" ;
      s[i][2] = " avg=" + df.format(colTot(i)/(double)rows());
      s[i][3] = " precision=" + colPre(i); 
    }
    int[] l = new int[4];
    for(int j=0;j<4;j++) 
      for(int k=0;k<columns();k++) 
        l[j] = Math.max(l[j], s[k][j].length());
    for(int k=0;k<columns();k++) {
      for(int j=0;j<4;j++) { 
        res+= s[k][j];
        int pad = l[j] - s[k][j].length();
        for(int m=0;m<=pad;m++) res+=" ";
      }
      res+="\n";
    }   
    return res;
  }
  public String head(int i) {
    String res ="";
    seek(0);
    double[] v = new double[columns()];
    for(Int it : this) {
      getRow(v);
      res += "[";
      for(double d : v) res += d+",";
      res += "]\n";
      if (i--==0) break;
    }
    return res;
  }
  
  static protected class WP { double [] weights, probabilities;  }
  final protected WP wp_ = new WP();
  static private double sum(double[] d) { double r=0.0; for(int i=0;i<d.length;i++) r+= d[i]; return r; }
  static private void normalize(double[] doubles, double sum) {
    assert ! Double.isNaN(sum) && sum != 0; for( int i=0; i<doubles.length; i++)  doubles[i]/=sum;
  }
  /// TODO: if the weights change we have to recompute...
  protected final WP wp(int rows_) {
    if(wp_.weights!=null) return wp_;
    wp_.weights = new double[rows_];
    for( int i = 0; i < wp_.weights.length; i++ ){
      seek(i);
      wp_.weights[i] = weight();
    }
    wp_.probabilities = new double[rows_];
    double sumProbs = 0, sumOfWeights = sum(wp_.weights);
    for( int i = 0; i < rows_; i++ ){
      sumProbs += random_.nextDouble();
      wp_.probabilities[i] = sumProbs;
    }
    normalize(wp_.probabilities, sumProbs / sumOfWeights);
    wp_.probabilities[rows_ - 1] = sumOfWeights;
    return wp_;
  }
  

  public static abstract class Wrap  extends Data {
    protected Data d_;
    public  Wrap(Data d)           { d_ = d; }
    public  void add(String c,  double v) {  d_.add(c, v);}
    public  void add(String c,  int v)    {  d_.add(c, v); }
    public  void add(int c, double v)     {  d_.add(c, v); }
    public  void add(int  c, int v){  d_.add(c,v); }
    public  Data shrinkWrap()      { return this; }
    public  void freeze()          { }
    public  int features()         { return d_.features(); }
    public  int columns()          { return d_.columns(); }
    public  int rows()             { return d_.rows(); }
    public  String name()          { return d_.name(); }
    public  int classOf()          { return d_.classOf(); }
    public  int classOf(int idx)          { return d_.classOf(idx); }
    public  int classes()          { return d_.classes(); }
    public  boolean hasNext()      { return d_.hasNext(); }
    public  Int next()             {  next._=d_.next()._; return next; }
    public  Data seek(int idx)     { return d_.seek(idx);}
    public  Data select(int from, int to) { return d_.select(from, to);}
    public  void addRow(double[] v){  d_.addRow(v);   }
    public  void getRow(double[] v){  d_.getRow(v); }
    public  int getI(int col)      { return d_.getI(col); }
    public  double getD(int col)   { return d_.getD(col); }
    public  String colName(int c)  { return d_.colName(c); }
    public  double colMin(int c)   { return d_.colMin(c); }
    public  double colMax(int c)   { return d_.colMax(c); }
    public  double colTot(int c)   { return d_.colTot(c); }
    public  int    colPre(int c)   { return d_.colPre(c); }    
    public String[] columnNames()  { return d_.columnNames(); }
    public String classColumnName(){ return d_.classColumnName(); }
   // public String toString() { //d_= d_.materialize();
    // d_.toString(); }
  }
  
  public class View extends Wrap {
    View(Data d) { super(d); }
    public  int classOf()          { return d_.classOf(); }
    public  boolean hasNext()      { return next._< d_.rows(); }
    public  Int next()             {  next._++; return next; }
    public  Data seek(int idx)     { next._=idx;return this; }
    public  void addRow(double[] v){  d_.addRow(v);   }
    public  void getRow(double[] v){  d_.getRow(v); }
    public  int getI(int col)      { return d_.getI(next._,col); }
    public  double getD(int col)   { return d_.getD(next._,col); }
    public String toString()       { return d_.toString(); }
    public void getRow(int col, double[] v) { d_.getRow(col,v); }
    protected int getI(int col, int idx) { return d_.getI(col,idx); }
    protected double getD(int col, int idx) { return d_.getD(col,idx); }
  }
}


class DataImpl extends Data {
    
    private Col[] c_;
    private HashMap<String, Integer> c2i_ = new HashMap<String,Integer>();
    private String name_="";
    private int classIdx_;
    private boolean frozen_;
    private int numClasses_=-1;
    private String[] columnNames_;
    private String classColumnName_;
    
    public DataImpl(String name, Object[] columns, String classNm) { this(columns,classNm); name_=name; } 
    public DataImpl(Object[] columns, String classNm){
      c_ = new Col[columns.length]; 
      columnNames_ = new String[columns.length];
      classColumnName_ = classNm;
      int i=0; for(Object o:columns) { 
        String s=o.toString();  columnNames_[i] = s; c_[i]=new Col.D(s); c2i_.put(s,i++); 
       }
      classIdx_ = c2i_.get(classNm);
    }
   

    DataImpl(DataImpl d_,int from, int to) {
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
    public int classOf() { return c_[classIdx_].getI(next._); }
    public int classOf(int idx) { return c_[classIdx_].getI(idx); }
    public int classes() {         
        if (!frozen_) throw new Error("Data set incomplete, freeze when done.");
        if (numClasses_==-1) 
            numClasses_= (int)c_[classIdx_].max_+1;           // c_[classIdx_].distribution().length;
        return numClasses_;
    }
    public Iterator<Int> iterator() { return this; }
    public boolean hasNext() { return c_.length>0 && next._ < c_[0].sz_; } 
    public Int next() { next._++; return next; }
    public void remove() { throw new Error("Unsported"); }
    public Data seek(int idx) { assert c_.length>0 && idx < c_[0].sz_;  next._ = idx; return this; }
    public  String colName(int c)  { return c_[c].name_; }
    public  double colMin(int c)  { return c_[c].min_; }    
    public  double colMax(int c)  { return c_[c].max_; }
    public  double colTot(int c)  { return c_[c].tot_; }
    public  int    colPre(int c)  { return c_[c].prec_; }
    
    public String[] columnNames() { return columnNames_; }
    public String   classColumnName() { return classColumnName_; }

    public Data select(int from, int to) { 
      DataImpl d = new DataImpl(this, from,to); d.name_+="->select("+from+","+to+")";      
      return d;
    }
   
   
    public void addRow(double[] v) {
      assert v.length==c_.length;
      if (frozen_) throw new Error("Frozen data set update");
      for(int i=0;i<v.length;i++)  c_[i].add(v[i]);
    }
    public void getRow(double[] v) { 
      assert v.length==c_.length;
      for(int i=0;i<v.length;i++) v[i] = c_[i].getD(next._);
    }
    public void getRow(int c, double[] v) { 
      assert v.length==c_.length;
      for(int i=0;i<v.length;i++) v[i] = c_[i].getD(c);
    }
    protected int getI(int col, int idx) { return c_[col].getI(idx); }
    protected double getD(int col, int idx) { return c_[col].getD(idx); }
    public int getI(int col) { return c_[col].getI(next._); }
    public double getD(int col) { return c_[col].getD(next._); }
    public String toString() {
      String res = super.toString();
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
    res+= "  ["+DataImpl.df.format(min_) +","+DataImpl.df.format(max_)+"], avg=";
    res+= DataImpl.df.format(tot_/(double)sz_) + " precision=" + prec_; 
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

class Subset extends Data.Wrap {
      
  int[] permutation_; // index of original rows
  int size_; // size of the sample
  
  public Subset(Data data, int size) {        
    super(data);  size_ =size; permutation_ = new int[size_];
  }
  
  void add(int i) { permutation_[next._++] = i; }

  public void freeze() { if (next._ != size_) throw new Error("wrong size"); }
  public  Data seek(int idx)               { next._ = idx; return this; }  
  public String name()                     { return d_.name() + "->subset"; }
  public  int rows()                       { return size_; }
  public  boolean hasNext()                { return next._ < size_-1; }
  public  Int next()                       { next._++; return next; }
  protected  int getI(int col, int idx)    { return d_.getI(col,permutation_[idx]); }
  protected  double getD(int col, int idx) { return d_.getD(col,permutation_[idx]); }
  public  int getI(int col)                { return d_.getI(col,permutation_[next._]); }
  public  double getD(int col)             { return d_.getD(col,permutation_[next._]); }    
  public  Data select(int from, int to)    { throw new Error("not implemented yet"); }
  public  void getRow(double[] v)          {  d_.getRow(permutation_[next._], v); } 
  public  void getRow(int c,double[] v)    {  d_.getRow(permutation_[c], v); }
  public int classOf()                     { return d_.classOf(permutation_[next._]); }
  public int classOf(int idx) { return d_.classOf(permutation_[idx]); }
}

class Filter extends Subset {

  Filter(Data d, Classifier c, int direction) {
    super(d,d.rows());    
    int count=0;
    d.seek(0);
    double[] v = new double[d.columns()];
    for(Int it : d) {
      d.getRow(v);
      if (c.navigate(v)==direction)
        count++;
      else
        permutation_[it._] = -1; 
    }
    int[] tmp = new int[count];
    int off=0;
    for(int i=0;i<permutation_.length;i++)
      if (permutation_[i]!=-1) tmp[off++]=i;
    permutation_ = tmp;
    size_= tmp.length;;
  }
}

class Shuffle extends Subset {
  Shuffle(Data d, int column){
    super(d, d.rows());
    for(int i=0;i<permutation_.length;i++) permutation_[i]=i;
    sort(permutation_,0,permutation_.length,column);
  }
  
  double get(int i, int c) { return d_.getD(c,i); }  
  //OJDK6
  private void sort(int x[], int off, int len,int column) {
    if (len < 7) {
        for (int i=off; i<len+off; i++)
            for (int j=i; j>off && get(x[j-1],column)>get(x[j],column); j--)  swap(x, j, j-1);
        return;
    }
    // Choose a partition element, v
    int m = off + (len >> 1);       // Small arrays, middle element
    if (len > 7) {
        int l = off;
        int n = off + len - 1;
        if (len > 40) {        // Big arrays, pseudomedian of 9
            int s = len/8;
            l = med3(x, l,     l+s, l+2*s,column);
            m = med3(x, m-s,   m,   m+s,column);
            n = med3(x, n-2*s, n-s, n,column);
        }
        m = med3(x, l, m, n,column); // Mid-size, med of 3
    }
    double v = get(x[m],column);
    // Establish Invariant: v* (<v)* (>v)* v*
    int a = off, b = a, c = off + len - 1, d = c;
    while(true) {
        while (b <= c && get(x[b],column) <= v) {
            if (get(x[b],column) == v) swap(x, a++, b);
            b++;
        }
        while (c >= b && get(x[c],column) >= v) {
            if (get(x[c],column) == v) swap(x, c, d--);
            c--;
        }
        if (b > c) break;
        swap(x, b++, c--);
    }
    // Swap partition elements back to middle
    int s, n = off + len;
    s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
    s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);
    // Recursively sort non-partition-elements
    if ((s = b-a) > 1)  sort(x, off, s,column);
    if ((s = d-c) > 1)  sort(x, n-s, s,column);
}
  private  int med3(int x[], int a, int b, int c,int column) {
      return (get(x[a],column) < get(x[b],column) ?
      (get(x[b],column) < get(x[c],column) ? b : get(x[a],column) < get(x[c],column) ? c : a) :
      (get(x[b],column) > get(x[c],column) ? b : get(x[a],column) > get(x[c],column) ? c : a));
  }
  private  void swap(int x[], int a, int b) { int t=x[a];x[a]=x[b];x[b]=t; }
  private  void vecswap(int x[], int a, int b, int n) {
      for (int i=0; i<n; i++, a++, b++)  swap(x, a, b);
  }
}


// PETA: Why not inherit from Subset? 

class Sample extends Data.Wrap {
      
  int[] permutation_; // index of original rows
  double bagSize_; // proportion of originals
  int size_; // size of the sample
  int seed_; // the seed used for sampling
  
  public Sample(Data data, double bagSize) {        
     super(data);
    bagSize_ = bagSize;
    size_ =(int)( data.rows() * bagSize_); 
    permutation_ = new int[size_];
    random_.setSeed(seed_=random_.nextInt()); // record the seed if we ever want to replay this tree 
    weightedSampling(data);        
  }

  public Data complement() {
    int[] orig = new int[d_.rows()];
    for(int i=0;i<permutation_.length;i++) orig[permutation_[i]]=-1;
    int sz=0;
    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) sz++;    
    Subset s = new Subset(d_,sz);
    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) s.add(i);     
    return s;
  }
  
  public  Data seek(int idx) { next._ = idx; return this; }  
  
  public String name() { return d_.name() + "->sampled(" + bagSize_+","+seed_+")"; }
    
  private void weightedSampling(Data d) {
    int sz = d.rows();
    WP wp = d.wp(sz);
    byte[] occurrences_ = new byte[sz];
    double[] weights = wp.weights, probabilities = wp.probabilities;
    int k = 0, l = 0, sumProbs = 0;
    while( k < sz && l < sz ){
      assert weights[l] > 0;
      sumProbs += weights[l];
      while( k < sz && probabilities[k] <= sumProbs ){  occurrences_[l]++; k++; }
      l++;
    }
    for(int i=0;i<permutation_.length;i++) {
      int offset = random_.nextInt(sz);
      while( true ){
        if( occurrences_[offset] != 0 ){occurrences_[offset]--; break; }
        offset = (offset + 1) % sz;
      }
      permutation_[i] = offset;      
    }
  }
  
  public  int rows()                       { return size_; }
  public  boolean hasNext()                { return next._ < size_-1; }
  public  Int next()                       { next._++; return next; }
  protected  int getI(int col, int idx)    { return d_.getI(col,permutation_[idx]); }
  protected  double getD(int col, int idx) { return d_.getD(col,permutation_[idx]); }
  public  int getI(int col)                { return d_.getI(col,permutation_[next._]); }
  public  double getD(int col)             { return d_.getD(col,permutation_[next._]); }    
  public  Data select(int from, int to)    { throw new Error("not implemented yet"); }
  public  void getRow(double[] v)          {  d_.getRow(next._, v); } 
  public  void getRow(int c,double[] v)    {  d_.getRow(permutation_[c], v); } 
  public int classOf() { return d_.classOf(permutation_[next._]); }
  public int classOf(int idx) { return d_.classOf(permutation_[idx]); }
}



