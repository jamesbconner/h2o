package hexlytics.data;

import hexlytics.Statistic.Split;
import hexlytics.data.Data.Row;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class Data implements Iterable<Row> {

  // Iterator implementation ---------------------------------------------------
  
  public class Row {  
    private double[] v_ = new double[columns()+1];
    public int index;
    public double weight;
    private boolean loaded_ = false;
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(index);
      sb.append(" ["+classOf()+"]:");
      for (double d: v()) 
        sb.append(" "+d);
      return sb.toString();
    }
    
    public double[] v() {
      if (!loaded_) {
        data_.getRow(index,v_);
        loaded_ = true;
      }
      return v_;
    }

    /** Returns the number of classes a row can have. */
    public int numClasses() { return classes(); }
    public int classOf() {
      return loaded_ ? (int)v_[v_.length-1] : data_.classOf(index);
    }
    
    public int getI(int colIndex) {
      return data_.getI(colIndex, index);
    }
    
    public double getD(int colIndex) {
      return data_.getD(colIndex,index);
    }
  }
 
  public class RowIter implements Iterator<Row> {
    final Row r = new Row();
    int pos = 0;
    public boolean hasNext() { return pos<rows(); }
    public Row next() {
      fillRow(r,pos);
      pos++;
      return r;
    }
    // H2O datasets are read only
    public void remove() { throw new Error("Unsported"); }
  }
  
  public Iterator<Row> iterator() {
    return new RowIter();
  } 
  
  /** Fills the given row object with data stored from the index-th row in the
   * current view. Note that the index is indexed in current data object's view
   */
  protected void fillRow(Row r, int rowIndex) {
    r.loaded_ = false;
    r.index = getPermutation(rowIndex);
    //data_.getRow(r.index,r.v_);
    //r.loaded_ = true;
    // TODO Change when datasets support weights
    r.weight = /*data_.weight() * */ getOriginalWeightAdjustments(r.index);
  }
  
  /** Returns the row with given index. */
  public Row getRow(int rowIndex) {
    Row result = new Row();
    fillRow(result,rowIndex);
    return result;
  }
  
  // prints --------------------------------------------------------------------
  
  public String rowIndicesToString() {
    StringBuilder sb = new StringBuilder();
    for (int i : getPermutation()) {
      sb.append(i);
      sb.append(" ");
    }
    return sb.toString();
  }
  
  // DataAdapter wrappers ------------------------------------------------------

  /** Returns the number of rows that is accessible by this Data object. 
   */
  public int rows() {
    return data_.rows();
  }
  
  public  int features()          { return data_.features(); }
  public  int columns()           { return data_.columns() -1 ; } // -1 to remove class column
  public  int classOf(int idx)    { return data_.classOf(idx); }
  public  int classes()           { return data_.classes(); }
// I'd rather have everything accessed using the rows for the sake of consistency
// and better control over what we access.   
//  public  void getRow(int col,double[] v) { data_.getRow(col, v); } 
//  public int getI(int col, int idx) { return data_.getI(col,idx); }
//  public double getD(int col, int idx) { return data_.getD(col,idx); }
//  public float weight(int idx)    { return 1; } 
  public Random random()          { return data_.random_; }
  public String colName(int c)    { return data_.colName(c); } 
  public double colMin(int c)     { return data_.colMin(c); }
  public double colMax(int c)     { return data_.colMax(c); }
  public double colTot(int c)     { return data_.colTot(c); }
  public int colPre(int c)        { return data_.colPre(c); }  
  public  String[] columnNames()  { return data_.columnNames(); }
  public  String classColumnName(){ return data_.classColumnName(); } 
  
  // Constructors --------------------------------------------------------------
  
  protected Data(DataAdapter da) {
    data_ = da;
    weightAdjustments_ = null;
    sortedCache_ = new SortedColumnsCache(da);
  }
  
  protected Data(Data data) {
    data_ = data.data_;
    weightAdjustments_ = data.weightAdjustments_;
    sortedCache_ = data.sortedCache_;
  }
  
  protected Data(DataAdapter da, int[] weightAdjustments) {
    data_ =da;
    weightAdjustments_ = weightAdjustments;
    sortedCache_ = new SortedColumnsCache(da);
  }
  
  /** Returns new Data object that stores all adapter's rows unchanged.
   */
  public static Data make(DataAdapter da) {
    return new Data(da);
  }

  // subsets -------------------------------------------------------------------
  
  public void filter(Split c, Data[] result) {
    int l=0, r=0;
    boolean[] tmp = new boolean[rows()];
    int i = 0;
    for(Row row : this) {
      if (row.getD(c.column) < c.value) {
//      if (row.v[c.column]<c.value) {
        tmp[i] = true; l++;
      } else {
        r++;
      }
      ++i;
    }
    int[] li = new int[l], ri = new int[r];
    l=0;r=0;
    for(i=0;i<tmp.length;i++) 
      if (tmp[i]) li[l++] = getPermutation(i);  else ri[r++]=getPermutation(i);
    result[0]= new Subset(this,li);
    result[1]= new Subset(this,ri);
  }
  
  public Data[] filter(Split c) {
    Data[] result = new Data[2];
    filter(c,result);
    return result;
  }
  
  // sampling ------------------------------------------------------------------   
  /** Creates a weighted sampling of the data. Uses the bag size given in
   * percentages in range 0 - 1.0. 
   */
  
  // data = old one 
  public Data sampleWithReplacement(double bagSizePct) {  
    int bagSize = (int)(rows() * bagSizePct);
    int sz = rows();
    // get the weights and probabilities
    wp();
    // get the random generator to initialize the sampling
    Random r = new Random(data_.random_.nextLong());
    // now sample
    byte[] occurrences_ = new byte[sz];
    int k = 0, l = 0, sumProbs = 0;
    while( k < sz && l < sz ){
      sumProbs += wp_.weights[l];
      while( k < sz && wp_.probabilities[k] <= sumProbs ) {
        occurrences_[l]++;
        k++;
      }
      l++;
    }
    // create our weight adjustment vector
    int[] w = new int[data_.rows()];
    int rowCount = 0;
    for(int i=0; i<bagSize; ++i) {
      int offset = r.nextInt(sz);
      while( true ) { 
        if( occurrences_[offset] != 0 ) {
          occurrences_[offset]--; 
          break;
        }
        offset = (offset + 1) % sz;
      }
      int origIndex = getPermutation(offset);
      w[origIndex] += 1;
      if (w[origIndex] == 1)
        ++rowCount;
    }
    // now collect the permutation
    int[] p = new int[rowCount];
    rowCount = 0;
    for (int i = 0; i < w.length; ++i)
      if (w[i]!=0)
        p[rowCount++] = i;
      else
        w[i] = 1; // correct the weights adjustment so that everyone is 1, important for the complement
    
    Subset sample = new Subset(this.data_,p,w);
    return sample;
  }  
  
  static protected class WP { double [] weights, probabilities;  }

  private final WP wp_ = new WP();
  
  static private double sum(double[] d) { double r=0.0; for(int i=0;i<d.length;i++) r+= d[i]; return r; }
  static private void normalize(double[] doubles, double sum) {
    assert ! Double.isNaN(sum) && sum != 0; for( int i=0; i<doubles.length; i++)  doubles[i]/=sum;
  }
  /// TODO: if the weights change we have to recompute...
  protected final WP wp() {
    if(wp_.weights!=null) return wp_;
    wp_.weights = new double[rows()];
    for( int i = 0; i < wp_.weights.length; i++ ){
      int oi = getPermutation(i);
      // TODO change when datasets support weights
      wp_.weights[i] = getOriginalWeightAdjustments(oi); //weight(i);
    }
    wp_.probabilities = new double[rows()];
    double sumProbs = 0, sumOfWeights = sum(wp_.weights);
    for( int i = 0; i < wp_.probabilities.length; i++ ){
      sumProbs += data_.random_.nextDouble();
      wp_.probabilities[i] = sumProbs;
    }
    normalize(wp_.probabilities, sumProbs / sumOfWeights);
    wp_.probabilities[wp_.probabilities.length - 1] = sumOfWeights;
    return wp_;
  }
  
  // sorting -------------------------------------------------------------------

  /** The cache for the sorted columns. 
   */
  static class SortedColumnsCache {
    
    // indices
    int[][] sortedIndices_;
    // sizes of the output arrays that must be created
    int[] outputSizes_;
    
    public SortedColumnsCache(DataAdapter data) {
      sortedIndices_ = new int[data.columns()][];
      outputSizes_ = new int[data.columns()];
    }
    
    public SortedColumnsCache(SortedColumnsCache old) {
      sortedIndices_ = new int[old.sortedIndices_.length][];
      outputSizes_ = new int[old.outputSizes_.length];
      System.arraycopy(old.sortedIndices_,0,sortedIndices_,0,sortedIndices_.length);
      System.arraycopy(old.outputSizes_,0,outputSizes_,0,outputSizes_.length);
    }
    
    public int storedOutputSize(int colIndex) {
      return outputSizes_[colIndex];
    } 
    
    public void setSortedByColumn(int colIndex, int[] rows, DataAdapter data) {
      outputSizes_[colIndex] = rows.length;
      int[] lastOccurrences = new int[data.rows()]; //sortedIndices_[colIndex] == null ? new int[data.rows()] : sortedIndices_[colIndex];
      for (int i = 0; i < lastOccurrences.length; ++i)
        lastOccurrences[i] = -1;
      for (int i = 0; i < rows.length; ++i) {
        if (lastOccurrences[rows[i]]!=-1)
          System.out.println("Duplicate row "+rows[i]);
        lastOccurrences[rows[i]] = i;
      }
      sortedIndices_[colIndex] = lastOccurrences;
      
//      // copy to the temporary as we might change it
//      int[] temp = new int[rows.length];
//      System.arraycopy(rows,0,temp,0,temp.length);
////      System.out.print("Before sort: ");
////      for (int i : temp)
////        System.out.print(i+", ");
////      System.out.println("");
//      int[] lastOccurrences = sortedIndices_[colIndex] == null ? new int[data.rows()] : sortedIndices_[colIndex];
//      for (int i = 0; i < lastOccurrences.length; ++i)
//        lastOccurrences[i] = -1;
//      for (int i = 0; i < temp.length; ++i) {
//        if (lastOccurrences[temp[i]]==-1) {
//          // we haven't seen the row yet, just store it
//          lastOccurrences[temp[i]] = i;
//          if (temp[i]==214972)
//            System.out.println(lastOccurrences[temp[i]]);
//        } else {
//          int row = temp[i];
//          // we have already seen the row, copy it to a position right after
//          // the last seen value
//          for (int j = lastOccurrences[row]+1 ; j <= i ; ++j) {
//            lastOccurrences[temp[j]] += 1;
//            temp[j] = temp[j-1];
//            if (temp[j]==214972)
//              System.out.println(lastOccurrences[temp[j]]);
//          }
//          if (lastOccurrences[row] >= temp.length)
//            System.out.println("error" + lastOccurrences[row]+" i="+i+" row="+row);
//          temp[lastOccurrences[row]] += 1;
//        }
//      }
//      // now in temp we have sorted row indices and in lastOccurrences we have
//      // the positions of the rows we need. 
//      sortedIndices_[colIndex] = lastOccurrences;
////      System.out.print("After sort:  ");
////      for (int i : temp)
////        System.out.print(i+", ");
////      System.out.println("");
    }
    
    private final boolean shouldSort(int colIndex, int rowSize, DataAdapter data) {
      if (outputSizes_[colIndex]<rowSize)
        return true;
      return (rowSize*Math.log(rowSize)) < (outputSizes_[colIndex]+rowSize);
    }
    
    public int[] getSortedByColumn(int colIndex, int[] rows, DataAdapter data) {
      if (shouldSort(colIndex,rows.length,data))
        return null;
      // do the non sort - for each row we have read its 
      int[] temp = new int[outputSizes_[colIndex]];
      for (int i: rows) {
        int j = sortedIndices_[colIndex][i]; // deal with copied rows
        if (j==-1)
          System.out.println("Unknown index "+i+" in column "+colIndex);
        if (temp[j] != 0) 
          System.out.println("Error here!");
        temp[j] = i+1; // to get rid of 0
      }
//      System.out.print("TEMP: ");
//      for (int i: temp) 
//        System.out.print(i+", ");
//      System.out.println("");
      // and do the compaction
      int[] result = new int[rows.length];
      int idx = 0;
      for (int i: temp) {
        if (i==0)
          continue;
        result[idx] = i-1;
        ++idx;
      }
      return result;
    }
  }
  
  SortedColumnsCache sortedCache_ = null;
  
  
  
  /** Returns the data sorted by the given column index. This function always
   * creates a new Data object. 
   */
  public Data sortByColumn(int colIdx) {
    Subset s = new Subset(this,getPermutation());
    s.sortPermutationByColumn(colIdx);
    return s;
  }

  /** Sorts the data in place if possible. If not possible returns new data
   * object. 
   */
  public Data sortByColumnInPlace(int colIdx) {
    Subset s = new Subset(this,getPermutation());
    s.sortPermutationByColumn(colIdx);
    return s;
  }
  
  // complement ----------------------------------------------------------------
  
  public Data complement() {
    int[] p = new int[data_.rows()-rows()];
    byte[] seen = new byte[data_.rows()];
    for (int i: getPermutation())
      seen[i] = 1;
    int offset = 0;
    for (int i=0; i< seen.length; ++i) {
      if (seen[i] == 0)
        p[offset++] = i;
    }
    return new Subset(this,p);
  }
  
  // Implementation ------------------------------------------------------------
  
  
  protected final DataAdapter data_;

  protected final int[] weightAdjustments_;
  
  /** Returns the array of the permutations of the original data adapter rows
   * as displayed by the given data object. 
   */
  protected int[] getPermutation() {
    int[] result = new int[rows()];
    for (int i = 0; i < result.length; ++i)
      result[i] = i;
    return result;
  }
  
  /** Returns the array of weight adjustments for the original data rows as
   * adjusted by the data object. Please note that the returned array must be
   * the same size as the number of original rows, not the number of rows
   * visible by the given data object. 
   */
  protected int[] getWeightAdjustments() {
    if (weightAdjustments_!=null)
      return weightAdjustments_;
    int[] result = new int[data_.rows()];
    for (int i = 0; i < result.length; ++i)
      result[i] = 1;
    return result;
  }

  /** Returns the original index of the row in the DataAdapter object for row
   * indexed in the given Data object. 
   */
  protected int getPermutation(int rowIndex) {
    return rowIndex;
  }
  
  /** Returns the weight adjustment for given row index. Please note that the
   * row index is the relative row index as specified by the data object, not
   * the original data row index. 
   */
  protected int getWeightAdjustment(int rowIndex) {
    if (weightAdjustments_==null)
      return 1;
    else
      return weightAdjustments_[getPermutation(rowIndex)];
  }
  
  /** Returns the weight adjustment of the given original row. 
   */
  protected int getOriginalWeightAdjustments(int originalRowIndex) {
    if (weightAdjustments_==null)
      return 1;
    else
      return weightAdjustments_[originalRowIndex];
  }
}

// Subset ----------------------------------------------------------------------

class Subset extends Data {
  
  protected int[] permutation_;
  
  /** Returns the original DataAdapter's row index of given row. */
  @Override protected int getPermutation(int rowIndex) {
    return permutation_[rowIndex];
  }
  
  /** We now have direct access to the permutation array. */
  @Override protected int[] getPermutation() {
    return permutation_;
  }
  
  /** Returns the number of rows. */
  @Override public int rows() {
    return permutation_.length;
  }
  
  /** Creates new subset of the given data adapter. The permutation is an array
   * of original row indices of the DataAdapter object that will be used. 
   */
  public Subset(Data data, int[] permutation) {
    super(data);
    permutation_ = permutation;
  }
  
  public Subset(DataAdapter data, int[] permutation, int[] weightAdjustments) {
    super(data, weightAdjustments);
    permutation_ = permutation;
  }
  
  // sorting implementation ----------------------------------------------------

  public void sortPermutationByColumn(int colIdx) {
    sort(permutation_,0,permutation_.length,colIdx);
    return;
/*    if (rows()<5000) {
      sort(permutation_,0,permutation_.length,colIdx);
      return;
    }
    //sort(permutation_,0,permutation_.length,colIdx);
    int p[] = sortedCache_.getSortedByColumn(colIdx, permutation_, data_);
    if (p == null) {
//      System.out.println("Sorting by column "+colIdx+" is faster");
      sort(permutation_,0,permutation_.length,colIdx);
//      if (sortedCache_.storedOutputSize(colIdx) > permutation_.length) {
//        System.out.println("  stored size "+sortedCache_.storedOutputSize(column)+" larger than "+permutation_.length+", copying...");
        sortedCache_ = new SortedColumnsCache(sortedCache_);
//      } else {
//      System.out.println("  storing "+permutation_.length+" rows sorted");
        sortedCache_.setSortedByColumn(colIdx, permutation_, data_);
//      }
    } else {
//      System.out.println("Using cache for column "+colIdx+" is faster...");
//      System.out.print("   ");
//      for (int i : permutation_)
//        System.out.print(i+", ");
//      System.out.println("");
//      System.out.print("   ");
//      for (int i : p)
//        System.out.print(i+", ");
//      System.out.println("");
      permutation_ = p;
    } */
  }

  public Data sortByColumnInPlace(int colIdx) {
    sort(permutation_,0,permutation_.length,colIdx);
    return this;
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

/**
 * The classes that mediate access to data. In the long run, we want these to support 
 * distributed data and lazy computation. We are mostly looking at getting the interface
 * right.
 * @author jan
 */
//public  class Data  implements Iterable<Row> {
//
//  /** The cache for the sorted columns. 
//   */
//  static class SortedColumnsCache {
//    
//    // indices
//    int[][] sortedIndices_;
//    // sizes of the output arrays that must be created
//    int[] outputSizes_;
//    
//    public SortedColumnsCache(DataAdapter data) {
//      sortedIndices_ = new int[data.columns()][];
//      outputSizes_ = new int[data.columns()];
//    }
//    
//    public SortedColumnsCache(SortedColumnsCache old) {
//      sortedIndices_ = new int[old.sortedIndices_.length][];
//      outputSizes_ = new int[old.outputSizes_.length];
//      System.arraycopy(old.sortedIndices_,0,sortedIndices_,0,sortedIndices_.length);
//      System.arraycopy(old.outputSizes_,0,outputSizes_,0,outputSizes_.length);
//    }
//    
//    public int storedOutputSize(int colIndex) {
//      return outputSizes_[colIndex];
//    } 
//    
//    public void setSortedByColumn(int colIndex, int[] rows, DataAdapter data) {
//      // copy to the temporary as we might change it
//      int[] temp = new int[rows.length];
//      System.arraycopy(rows,0,temp,0,temp.length);
////      System.out.print("Before sort: ");
////      for (int i : temp)
////        System.out.print(i+", ");
////      System.out.println("");
//      int[] lastOccurrences = sortedIndices_[colIndex] == null ? new int[data.rows()] : sortedIndices_[colIndex];
//      for (int i = 0; i < lastOccurrences.length; ++i)
//        lastOccurrences[i] = -1;
//      for (int i = 0; i < temp.length; ++i) {
//        if (lastOccurrences[temp[i]]==-1) {
//          // we haven't seen the row yet, just store it
//          lastOccurrences[temp[i]] = i;
//          if (temp[i]==214972)
//            System.out.println(lastOccurrences[temp[i]]);
//        } else {
//          int row = temp[i];
//          // we have already seen the row, copy it to a position right after
//          // the last seen value
//          for (int j = lastOccurrences[row]+1 ; j <= i ; ++j) {
//            lastOccurrences[temp[j]] += 1;
//            temp[j] = temp[j-1];
//            if (temp[j]==214972)
//              System.out.println(lastOccurrences[temp[j]]);
//          }
//          if (lastOccurrences[row] >= temp.length)
//            System.out.println("error" + lastOccurrences[row]+" i="+i+" row="+row);
//          temp[lastOccurrences[row]] += 1;
//        }
//      }
//      // now in temp we have sorted row indices and in lastOccurrences we have
//      // the positions of the rows we need. 
//      sortedIndices_[colIndex] = lastOccurrences;
////      System.out.print("After sort:  ");
////      for (int i : temp)
////        System.out.print(i+", ");
////      System.out.println("");
//    }
//    
//    private final boolean shouldSort(int colIndex, int rowSize, DataAdapter data) {
//      if (outputSizes_[colIndex]<rowSize)
//        return true;
//      return (rowSize*Math.log(rowSize)) < (outputSizes_[colIndex]+rowSize);
//    }
//    
//    public int[] getSortedByColumn(int colIndex, int[] rows, DataAdapter data) {
//      if (shouldSort(colIndex,rows.length,data))
//        return null;
//      // do the non sort - for each row we have read its 
//      int[] temp = new int[outputSizes_[colIndex]];
//      for (int i: rows) {
//        int j = sortedIndices_[colIndex][i]; // deal with copied rows
//        if (j==-1)
//          System.out.println("Unknown index "+i+" in column "+colIndex);
//        while (temp[j]!=0) {
//          --j;
//          if (j==-1)
//            System.out.println("error");
//        }
//        temp[j] = i+1; // to get rid of 0
//      }
////      System.out.print("TEMP: ");
////      for (int i: temp) 
////        System.out.print(i+", ");
////      System.out.println("");
//      // and do the compaction
//      int[] result = new int[rows.length];
//      int idx = 0;
//      for (int i: temp) {
//        if (i==0)
//          continue;
//        result[idx] = i-1;
//        ++idx;
//      }
//      return result;
//    }
//  }
//  
//  SortedColumnsCache sortedCache_ = null;
//  
//  static final DecimalFormat df = new  DecimalFormat ("0.##");
//  public static Random RANDOM = new Random(42);
// 
//  public static Data make(DataAdapter da) { return new Data(da); }    
//
//  Data(Data d) {
//    this(d.data_);
//    sortedCache_ = d.sortedCache_;
//  }
//  
//  Data(DataAdapter da) {
//    data_ = da;
//    name_=data_.name();
//    sortedCache_ = new SortedColumnsCache(da);
//  }
//  
//  /** Returns the random generator associated with the used adapter. */
//  public Random random() {
//    return data_.random_;
//  }
//
//  /** Returns the original index to the data. Should be redefined in subclasses that
//   * change or shuffle the indices.
//   * 
//   * @param idx Index for this data
//   * @return Original index of the row (the index on DataAdapter). 
//   */
//  public int originalIndex(int idx) {
//    return idx;
//  }
//  
//  public class Row {  
//    public double[] v = new double[columns()+1];
//    public int index;
//    
//    public String toString() {
//      StringBuilder sb = new StringBuilder();
//      sb.append(index);
//      sb.append(" ["+classOf()+"]:");
//      for (double d: v) 
//        sb.append(" "+d);
//      return sb.toString();
//    }
//
//    /** Returns the number of classes a row can have. */
//    public int numClasses() { return classes(); }
//    public int classOf() { return (int)v[v.length-1]; }
//  }
// 
//  public class RowIter implements Iterator<Row> {
//    final Row r = new Row();
//    int pos = 0;
//    public boolean hasNext() { return pos<rows(); }
//    public Row next() {
//      data_.getRow(pos, r.v);
//      r.index=pos++;
//      return r;
//    }
//    public void remove() { throw new Error("Unsported"); }
//  }
// 
//  final DataAdapter data_;   
//  String name_;
//  
//  public  Iterator<Row> iterator(){ return new RowIter(); } 
//  public  int features()          { return data_.features(); }
//  public  int columns()           { return data_.columns() -1 ; } // -1 to remove class column
//  public  int rows()              { return data_.rows(); }
//  public  String name()           { return name_; }
//  public  int classOf(int idx)    { return data_.classOf(idx); }
//  public  int classes()           { return data_.classes(); }
//  public  void getRow(int col,double[] v) { data_.getRow(col, v); } 
//  public int getI(int col, int idx) { return data_.getI(col,idx); }
//  public double getD(int col, int idx) { return data_.getD(col,idx); }
//  public float weight(int idx)    { return 1; } 
//  public String colName(int c)    { return data_.colName(c); } 
//  public double colMin(int c)     { return data_.colMin(c); }
//  public double colMax(int c)     { return data_.colMax(c); }
//  public double colTot(int c)     { return data_.colTot(c); }
//  public int colPre(int c)        { return data_.colPre(c); }  
//  public  String[] columnNames()  { return data_.columnNames(); }
//  public  String classColumnName(){ return data_.classColumnName(); } 
//  public Data complement()        { throw new Error("Unsupported"); }
//  public Data sampleWithReplacement(double bagSize) {
//    assert bagSize > 0 && bagSize <= 1.0;     
//    return new Sample(this,bagSize); 
//  } 
//  public Data sort(int column) { return new Shuffle(this, column); }
//
//  public String toString() {
//    String res = "Data "+ name()+"\n";
//    if (columns()>0) { res+= rows()+" rows, "+ columns() + " cols, "+ classes() +" classes\n"; }
//    String[][] s = new String[columns()][4];
//    for(int i=0;i<columns();i++){
//      s[i][0] = "col("+colName(i)+")";
//      s[i][1] = "["+df.format(colMin(i)) +","+df.format(colMax(i))+"]" ;
//      s[i][2] = " avg=" + df.format(colTot(i)/(double)rows());
//      s[i][3] = " precision=" + colPre(i); 
//    }
//    int[] l = new int[4];
//    for(int j=0;j<4;j++) 
//      for(int k=0;k<columns();k++) 
//        l[j] = Math.max(l[j], s[k][j].length());
//    for(int k=0;k<columns();k++) {
//      for(int j=0;j<4;j++) { 
//        res+= s[k][j]; int pad = l[j] - s[k][j].length();
//        for(int m=0;m<=pad;m++) res+=" ";
//      }
//      res+="\n";
//    }      
//    res +="========\n";  res +="class histogram\n";
//    int[] dist = data_.c_[data_.classIdx_].distribution();
//    int[] sorted = Arrays.copyOf(dist, dist.length);
//    Arrays.sort(sorted);
//    int max = sorted[sorted.length-1];
//    int[] prop = new int[dist.length];
//    for(int j=0;j<prop.length;j++)
//      prop[j]= (int) (10.0 * ( (float)dist[j]/max )); 
//    for(int m=10;m>=0;m--) {
//      for(int j=0;j<prop.length;j++){
//        if (prop[j]>= m) res += "**  "; else res+="    ";
//      }
//      res+="\n";
//    }
//    res+="[";
//    for(int j=0;j<dist.length;j++) res+=dist[j]+((j==dist.length-1)?"":",");     
//    res+="]\n"; res+=head(5);
//    return res;
//  }
//  
//  public String head(int i) {
//    String res ="";
//    for(Row r : this) {
//      res += "["; for(double d : r.v) res += d+","; res += "]\n";
//      if (i--==0) break;
//    }
//    return res;
//  }
//  
//  static protected class WP { double [] weights, probabilities;  }
//  final protected WP wp_ = new WP();
//  static private double sum(double[] d) { double r=0.0; for(int i=0;i<d.length;i++) r+= d[i]; return r; }
//  static private void normalize(double[] doubles, double sum) {
//    assert ! Double.isNaN(sum) && sum != 0; for( int i=0; i<doubles.length; i++)  doubles[i]/=sum;
//  }
//  /// TODO: if the weights change we have to recompute...
//  protected final WP wp(int rows_) {
//    if(wp_.weights!=null) return wp_;
//    wp_.weights = new double[rows_];
//    for( int i = 0; i < wp_.weights.length; i++ ){
//      wp_.weights[i] = weight(i);
//    }
//    wp_.probabilities = new double[rows_];
//    double sumProbs = 0, sumOfWeights = sum(wp_.weights);
//    for( int i = 0; i < rows_; i++ ){
//      sumProbs += data_.random_.nextDouble();
//      wp_.probabilities[i] = sumProbs;
//    }
//    normalize(wp_.probabilities, sumProbs / sumOfWeights);
//    wp_.probabilities[rows_ - 1] = sumOfWeights;
//    return wp_;
//  }
// 
//  
//  public void filter(Split c, Data[] result) {
//    int l=0, r=0;
//    boolean[] tmp = new boolean[rows()];
//    for(Row row : this) {
//      if (row.v[c.column]<c.value) {
//        tmp[row.index] = true; l++;
//      } else {
//        r++;
//      }
//    }
//    int[] li = new int[l], ri = new int[r];
//    l=0;r=0;
//    for(int i=0;i<tmp.length;i++) 
//      if (tmp[i]) li[l++] = originalIndex(i);  else ri[r++]=originalIndex(i);
//    result[0]= new Subset(this,li);
//    result[1]= new Subset(this,ri);
//  }
//}
//
//class Subset extends Data {     
//  int[] permutation_; // index of original rows  
//
//  public int originalIndex(int idx) {
//    return permutation_[idx];
//  }
//  
//  Subset(Data d, int size) {   
//    
//    super(d);  permutation_ = new int[size]; name_ =d.name_+"->subset"; 
//    if (permutation_.length==0) throw new Error("creating zero sized subset is not supported");
//  }
//  Subset(Data d, int[] perm) {        
//    super(d);
//    permutation_ = perm; name_ =d.name_+"->subset";
//    if (permutation_.length==0)
//      throw new Error("creating zero sized subset is not supported");
//  }
// 
//  public class IterS extends RowIter {
//    public Row next() {
//      data_.getRow(permutation_[pos], r.v);
//      r.index=pos++;
//      return r;
//    }
//  }
//  
//  public  RowIter iterator()            { return new IterS(); }   
//  public  int rows()                    { return permutation_.length; }
//  public  int getI(int col, int idx)    { return data_.getI(col,permutation_[idx]); }
//  public  double getD(int col, int idx) { return data_.getD(col,permutation_[idx]); }
//  public  void getRow(int c,double[] v) {  data_.getRow(permutation_[c], v); }
//  public int classOf(int idx)           { return data_.classOf(permutation_[idx]); }
//  
//  public String toString() {
//    DataAdapter a = new DataAdapter(name(),columnNames(),classColumnName());
//    for(Row r : this) a.addRow(r.v);
//    a.freeze();
//    return new Data(a).toString();
//  }
//  
//}
//
//
//class Shuffle extends Subset {
//  
//  Shuffle(Data d, int column) {
//    super(d, d.rows());
//    if (d instanceof Subset) {
//      Subset sd = (Subset)d;
//      System.arraycopy(sd.permutation_, 0, permutation_, 0, permutation_.length);
//    } else {
//      for (int i = 0; i< d.rows(); ++i)
//        permutation_[i] = d.originalIndex(i);
//    }
//// old code & comments kept for the time baing for debugging purposes    
////    sort(permutation_,0,permutation_.length,column);
//  
//    int p[] = sortedCache_.getSortedByColumn(column, permutation_, data_);
//    if (p == null) {
////      System.out.println("Sorting by column "+column+" is faster.");
//      sort(permutation_,0,permutation_.length,column);
//      if (sortedCache_.storedOutputSize(column) > permutation_.length) {
////        System.out.println("  stored size "+sortedCache_.storedOutputSize(column)+" larger than "+permutation_.length+", copying...");
//        sortedCache_ = new SortedColumnsCache(sortedCache_);
//      }
////      System.out.println("  storing "+permutation_.length+" rows sorted");
//      sortedCache_.setSortedByColumn(column, permutation_, data_);
//    } else {
////      System.out.println("Using cache for column "+column+" is faster...");
////      System.out.print("   ");
////      for (int i : permutation_)
////        System.out.print(i+", ");
////      System.out.println("");
////      System.out.print("   ");
////      for (int i : p)
////        System.out.print(i+", ");
////      System.out.println("");
//      permutation_ = p;
//    }
//  }
//  
//  double get(int i, int c) { return data_.getD(c,i); }  
//  //OJDK6
//  private void sort(int x[], int off, int len,int column) {
//    if (len < 7) {
//        for (int i=off; i<len+off; i++)
//            for (int j=i; j>off && get(x[j-1],column)>get(x[j],column); j--)  swap(x, j, j-1);
//        return;
//    }
//    // Choose a partition element, v
//    int m = off + (len >> 1);       // Small arrays, middle element
//    if (len > 7) {
//        int l = off;
//        int n = off + len - 1;
//        if (len > 40) {        // Big arrays, pseudomedian of 9
//            int s = len/8;
//            l = med3(x, l,     l+s, l+2*s,column);
//            m = med3(x, m-s,   m,   m+s,column);
//            n = med3(x, n-2*s, n-s, n,column);
//        }
//        m = med3(x, l, m, n,column); // Mid-size, med of 3
//    }
//    double v = get(x[m],column);
//    // Establish Invariant: v* (<v)* (>v)* v*
//    int a = off, b = a, c = off + len - 1, d = c;
//    while(true) {
//        while (b <= c && get(x[b],column) <= v) {
//            if (get(x[b],column) == v) swap(x, a++, b);
//            b++;
//        }
//        while (c >= b && get(x[c],column) >= v) {
//            if (get(x[c],column) == v) swap(x, c, d--);
//            c--;
//        }
//        if (b > c) break;
//        swap(x, b++, c--);
//    }
//    // Swap partition elements back to middle
//    int s, n = off + len;
//    s = Math.min(a-off, b-a  );  vecswap(x, off, b-s, s);
//    s = Math.min(d-c,   n-d-1);  vecswap(x, b,   n-s, s);
//    // Recursively sort non-partition-elements
//    if ((s = b-a) > 1)  sort(x, off, s,column);
//    if ((s = d-c) > 1)  sort(x, n-s, s,column);
//}
//  private  int med3(int x[], int a, int b, int c,int column) {
//      return (get(x[a],column) < get(x[b],column) ?
//      (get(x[b],column) < get(x[c],column) ? b : get(x[a],column) < get(x[c],column) ? c : a) :
//      (get(x[b],column) > get(x[c],column) ? b : get(x[a],column) > get(x[c],column) ? c : a));
//  }
//  private  void swap(int x[], int a, int b) { int t=x[a];x[a]=x[b];x[b]=t; }
//  private  void vecswap(int x[], int a, int b, int n) {
//      for (int i=0; i<n; i++, a++, b++)  swap(x, a, b);
//  }
//}
////
//class Sample extends Subset {
//  
//  public final long seed;
//  
//  public Sample(Data data, double bagSize) {        
//    super(data,(int)( data.rows() * bagSize));
//    // create a new temporary random with the given seed, so that it can be
//    // replayed if necessary
//    seed =data_.random_.nextLong();
//    weightedSampling(data,new Random(seed));
//    name_ = data.name() + "->sampled(" + bagSize+","+seed+")"; 
//  }
//
//  public Data complement() {
//    int[] orig = new int[data_.rows()];
//    for(int i=0;i<permutation_.length;i++) orig[permutation_[i]]=-1;
//    int sz=0;
//    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) sz++;    
//    int[] tmp = new int[sz]; int off=0;
//    for(int i=0;i<orig.length;i++) if(orig[i]!=-1) tmp[off++]=i;
//    Subset s = new Subset(this,tmp);    
//    return s;
//  }
//    
//  private void weightedSampling(Data d,Random r) {
//    int sz = d.rows();
//    WP wp = d.wp(sz);
//    byte[] occurrences_ = new byte[sz];
//    double[] weights = wp.weights, probabilities = wp.probabilities;
//    int k = 0, l = 0, sumProbs = 0;
//    while( k < sz && l < sz ){
//      assert weights[l] > 0;
//      sumProbs += weights[l];
//      while( k < sz && probabilities[k] <= sumProbs ){  occurrences_[l]++; k++; }
//      l++;
//    }
//    for(int i=0;i<permutation_.length;i++) {
//      int offset = r.nextInt(sz);
//      while( true ){
//        if( occurrences_[offset] != 0 ){occurrences_[offset]--; break; }
//        offset = (offset + 1) % sz;
//      }
//      permutation_[i] = offset;      
//    }
//  }  
//}
//
//
//
