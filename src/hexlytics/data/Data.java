package hexlytics.data;

import hexlytics.Statistic.Split;
import hexlytics.data.Data.Row;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
/**
 * The classes that mediate access to data. In the long run, we want these to support 
 * distributed data and lazy computation. We are mostly looking at getting the interface
 * right.
 * @author jan
 */
public  class Data  implements Iterable<Row> {
  
  static long SEED_;
  static final DecimalFormat df = new  DecimalFormat ("0.##");
  public static Random RANDOM = new Random(42);
  public static void setSeed(long seed) { SEED_ = seed; RANDOM=new Random(SEED_); } 
 
  public static Data make(DataAdapter da) { return new Data(da); }    

  Data(DataAdapter da) { data_ = da; name_=data_.name(); }

  /** Returns the original index to the data. Should be redefined in subclasses that
   * change or shuffle the indices.
   * 
   * @param idx Index for this data
   * @return Original index of the row (the index on DataAdapter). 
   */
  protected int originalIndex(int idx) {
    return idx;
  }
  
  
  public class Row {  
    public double[] v = new double[columns()];
    public int classOf;
    public int index;
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(index);
      sb.append(" ["+classOf+"]:");
      for (double d: v) 
        sb.append(" "+d);
      return sb.toString();
    }
  }
 
  public class RowIter implements Iterator<Row> {
    final Row r = new Row();
    int pos = 0;
    public boolean hasNext() { return pos<rows(); }
    public Row next() {
      data_.getRow(pos, r.v);
      r.classOf=data_.classOf(pos);
      r.index=pos++;
      return r;
    }
    public void remove() { throw new Error("Unsported"); }
  }
 
  final DataAdapter data_;   
  String name_;
  public final Random random_ = new Random(RANDOM.nextLong());
  
  public  Iterator<Row> iterator(){ return new RowIter(); } 
  public  int features()          { return data_.features(); }
  public  int columns()           { return data_.columns(); }
  public  int rows()              { return data_.rows(); }
  public  String name()           { return name_; }
  public  int classOf(int idx)    { return data_.classOf(idx); }
  public  int classes()           { return data_.classes(); }
  public  void getRow(int col,double[] v) { data_.getRow(col, v); } 
  public int getI(int col, int idx) { return data_.getI(col,idx); }
  public double getD(int col, int idx) { return data_.getD(col,idx); }
  public float weight(int idx)    { return 1; } 
  public String colName(int c)    { return data_.colName(c); } 
  public double colMin(int c)     { return data_.colMin(c); }
  public double colMax(int c)     { return data_.colMax(c); }
  public double colTot(int c)     { return data_.colTot(c); }
  public int colPre(int c)        { return data_.colPre(c); }  
  public  String[] columnNames()  { return data_.columnNames(); }
  public  String classColumnName(){ return data_.classColumnName(); } 
  public Data complement()        { throw new Error("Unsupported"); }
  public Data sampleWithReplacement(double bagSize) {
    assert bagSize > 0 && bagSize <= 1.0;     
    return new Sample(this,bagSize); 
  } 
  public Data sort(int column) { return new Shuffle(this, column); }

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
        res+= s[k][j]; int pad = l[j] - s[k][j].length();
        for(int m=0;m<=pad;m++) res+=" ";
      }
      res+="\n";
    }   
    res +="========\n";  res +="class histogram\n";
    int[] dist = data_.c_[data_.classIdx_].distribution();
    int[] sorted = Arrays.copyOf(dist, dist.length);
    Arrays.sort(sorted);
    int max = sorted[sorted.length-1];
    int[] prop = new int[dist.length];
    for(int j=0;j<prop.length;j++)
      prop[j]= (int) (10.0 * ( (float)dist[j]/max )); 
    for(int m=10;m>=0;m--) {
      for(int j=0;j<prop.length;j++){
        if (prop[j]>= m) res += "**  "; else res+="    ";
      }
      res+="\n";
    }
    res+="[";
    for(int j=0;j<dist.length;j++) res+=dist[j]+((j==dist.length-1)?"":",");     
    res+="]\n"; res+=head(5);
    return res;
  }
  
  public String head(int i) {
    String res ="";
    for(Row r : this) {
      res += "["; for(double d : r.v) res += d+","; res += "]\n";
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
      wp_.weights[i] = weight(i);
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
 
  
  public void filter(Split c, Data[] result) {
    int l=0, r=0;
    boolean[] tmp = new boolean[rows()];
    for(Row row : this) {
      if (row.v[c.column]<c.value) {
        tmp[row.index] = true; l++;
      } else {
        r++;
      }
    }
    int[] li = new int[l], ri = new int[r];
    l=0;r=0;
    for(int i=0;i<tmp.length;i++) 
      if (tmp[i]) li[l++] = originalIndex(i);  else ri[r++]=originalIndex(i);
    result[0]= new Subset(this,li);
    result[1]= new Subset(this,ri);
  }
}

class Subset extends Data {     
  int[] permutation_; // index of original rows  

  protected int originalIndex(int idx) {
    return permutation_[idx];
  }
  
  Subset(Data d, int size) {        
    super(d.data_);  permutation_ = new int[size]; name_ =d.name_+"->subset"; 
    if (permutation_.length==0) throw new Error("creating zero sized subset is not supported");
  }
  Subset(Data d, int[] perm) {        
    super(d.data_);
    permutation_ = perm; name_ =d.name_+"->subset";
    if (permutation_.length==0)
      throw new Error("creating zero sized subset is not supported");
  }
 
  public class IterS extends RowIter {
    public Row next() {
      data_.getRow(permutation_[pos], r.v);
      r.classOf=data_.classOf(permutation_[pos]);
      r.index=pos++;
      return r;
    }
  }
  
  public  RowIter iterator()            { return new IterS(); }   
  public  int rows()                    { return permutation_.length; }
  public  int getI(int col, int idx)    { return data_.getI(col,permutation_[idx]); }
  public  double getD(int col, int idx) { return data_.getD(col,permutation_[idx]); }
  public  void getRow(int c,double[] v) {  data_.getRow(permutation_[c], v); }
  public int classOf(int idx)           { return data_.classOf(permutation_[idx]); }
  
  public String toString() {
    DataAdapter a = new DataAdapter(name(),columnNames(),classColumnName());
    for(Row r : this) a.addRow(r.v);
    a.freeze();
    return new Data(a).toString();
  }
}


class Shuffle extends Subset {
  Shuffle(Data d, int column){
    super(d, d.rows());
    if (d instanceof Subset) {
      Subset sd = (Subset)d;
      System.arraycopy(sd.permutation_, 0, permutation_, 0, permutation_.length);
    } else {
      for (int i = 0; i< d.rows(); ++i)
        permutation_[i] = d.originalIndex(i);
    }
    //for(int i=0;i<permutation_.length;i++) permutation_[i]=i;
    sort(permutation_,0,permutation_.length,column);
  }
  
  double get(int i, int c) { return data_.getD(c,i); }  
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

class Sample extends Subset {
  
  public Sample(Data data, double bagSize) {        
    super(data,(int)( data.rows() * bagSize));
    int seed =random_.nextInt();
    random_.setSeed(seed); // record the seed if we ever want to replay this tree 
    weightedSampling(data);
    name_ = data.name() + "->sampled(" + bagSize+","+seed+")"; 
  }

  public Data complement() {
    int[] orig = new int[data_.rows()];
    for(int i=0;i<permutation_.length;i++) orig[permutation_[i]]=-1;
    int sz=0;
    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) sz++;    
    int[] tmp = new int[sz]; int off=0;
    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) tmp[off++]=i;
    Subset s = new Subset(this,tmp);    
    return s;
  }
    
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
}



