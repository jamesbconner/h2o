package analytics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Split on numerical values using entropy to find the best split. 
 *  While the algorithm is multi-pass, it is all created in the single pass 
 *  of the RF we have. The classic pass only analyzes the distribution for the node.
 * 
 * When done, the build classifier of the node revisits those rows that were
 * used for the node, sorts them for each column and then looks at each possible
 * split point to determine the best one. 
 * 
 * The best split point across selected columns on the node will be used and
 * the two parent nodes generated. 
 *
 * @author peta
 */
public class NumericSplitterStatistic extends Statistic {
 
  /** Enables reporting of the gain function. */
  public static final boolean ENABLE_GAIN_REPORTING = true;
  
  private int[] rows_; // the node's rows
  private int rowsSize_;
  private final DataAdapter data;  // data
  private final ColumnStatistic[] columns_;  //columns for which the averages are computed
  

  
  /** Hold information about a split. */
  static class SplitInfo {
    final int column; final double value, fitness;    
    SplitInfo(int column, double splitValue, double fitness) {
      this.column = column;  this.value = splitValue; this.fitness = fitness;
    }    
    boolean betterThan(SplitInfo other) { return other==null || fitness > other.fitness;  }
  }

  static BufferedWriter gains=null;

  public static void openForGainBuffering(String filename) {
    if (ENABLE_GAIN_REPORTING)
      try { 
        gains = new BufferedWriter(new FileWriter(filename));
      } catch( IOException ex ) {
        System.err.println("Cannot create or append the gains file "+filename);
      }
  }
  
  public static void closeGainReporting() {
    if (ENABLE_GAIN_REPORTING)
      if (gains!=null)
        try { gains.close(); } catch (IOException e) { }
  }
  
  /** Computer  the statistic on each column; holds the distribution of the values 
   * using weights. This class is called from the main statistic for each column 
   * the statistic cares about. */
  class ColumnStatistic {
    byte column; // column
    double[][] dists; // 2 x numClasses
    double[][] bestDists; // distribution for the temporary node
  
    ColumnStatistic(int index, int numClasses) {
      this.column = (byte)index;
      dists = new double[2][numClasses];
      bestDists = new double[2][numClasses];
    }
    
    void addDataPoint(DataAdapter row) { dists[1][row.dataClass()] += row.weight(); }    
    
    /** Calculates the best split on given column and returns its information.
     * 
     * In order to do this it must sort the rows for the given node and then
     * walk again over them in order (that is out of their natural order) to
     * determine the best splitpoint.
     */
    SplitInfo bestSplit() {
      double[] fits = null;
      if ((ENABLE_GAIN_REPORTING) && (gains!=null))
        fits = new double[rowsSize_];
      double fit = Utils.entropyOverColumns(dists); //compute fitness with no prediction
      sort1(rows_,0,rowsSize_,column); // sort rows_ according to given column      
      double last = data.seekToRow(rows_[rowsSize_-1]).toDouble(column);
      double currSplit =  data.seekToRow(rows_[0]).toDouble(column);
      if (last == currSplit) return null;
      // now try all the possible splits
      double bestFit = -Double.MAX_VALUE;
      double split = 0;
      double gain = 0;
      int gi = 0;
      for (int i =0; i< rowsSize_; i++) {
        double s = data.seekToRow(rows_[i]).toDouble(column);
        if (s > currSplit) {
          gain = Utils.entropyCondOverRows(dists); // fitness gain
          if ((ENABLE_GAIN_REPORTING) && (gains!=null)) {
            fits[gi] = gain;
            ++gi;
          }
          
          double newFit = fit - gain; // fitness gain
          if (newFit > bestFit) {
            bestFit = newFit;
            split = (s + currSplit) / 2;
            for (int ii = 0; ii < 2; ++ii) for (int iii = 0; iii < bestDists.length; ++iii)
              bestDists[ii][iii] = dists[ii][iii];
          }
        }
        currSplit = s;
        dists[0][data.dataClass()] += data.weight();
        dists[1][data.dataClass()] -= data.weight();
      }
      if ((ENABLE_GAIN_REPORTING) && (gains!=null))
        try {
          synchronized (gains) {
            gains.write(String.valueOf(fits.length));
            gains.write(" "+gi);
            gains.write(" "+fit);
            for (double d: fits) {
              --gi;
              if (gi<=0)
                break;
              gains.write(" "+d);
            }
            gains.write("\n");
          }
        } catch (IOException ex) {
          // pass
        }
      return new SplitInfo(column,split,bestFit);
    }
    
  }
  
  /** Adds the data point to all column statistics
   * 
   * @param data 
   */
  @Override public void addRow(DataAdapter data) {
    for (ColumnStatistic s: columns_)
      s.addDataPoint(data);
    // add the row to the list of rows valid for this node. 
    grow();
    rows_[rowsSize_++] = data.row();
  }
  private final void grow() { 
    if (rowsSize_==rows_.length) {
      int sz = rowsSize_ * 2;      
      rows_ = Arrays.copyOf(rows_, sz > data.numRows()? data.numRows() : sz);
    }
  }
  /** Creates the classifier. If the node has seen only single data class, a
   * const classifier is used. Otherwise all columns are queried to find the
   * best split. If the chosen selected columns is not able to differentiate
   * between the observations, a Const node will be returned with a majority
   * vote for the class.
   */
  @Override public Classifier createClassifier() {
    if (rowsSize_==0) throw new Error();
    else {     // check if we have only one class
      int cls = 0, cnt = 0;  double[] vs = columns_[0].dists[1];
      for (int i = 0; i<vs.length;++i) if (vs[i]!=0) { cnt++; cls = i;}
      if (cnt==1) return new Classifier.Const(cls);   
    }
    SplitInfo best = null;
    for (ColumnStatistic s: columns_) {
      SplitInfo x = s.bestSplit();
      if (x!=null && x.betterThan(best)) best = x;
    }
    //for all chosen columns, all observations have the same values and 
    //no split was selected. In this case we give up and create a Const node. 
    if (best==null) {
      double max = 0; int index= 0; double[] vs = columns_[0].dists[1];
      for (int j = 0; j<vs.length;++j) if (vs[j]>max) {index=j; max=vs[j];}
      return new Classifier.Const(index);            
    }
    return new SplitClassifier(best); 
  }
  
  /** Returns the temporary classifier that can be used for the node if not all
   * its children are created.
   * 
   * Uses the random classifier over the distribution on the given node.
   * 
   * @return 
   */
  public Classifier createTemporaryClassifier(int subnode) {
    return new Classifier.Random(columns_[0].bestDists[subnode]);
  }


  public NumericSplitterStatistic(DataAdapter data) {
    rows_ = new int[100];
    columns_ = new ColumnStatistic[data.numFeatures()];
    this.data = data;
    pickColumns();
  }

  private void pickColumns() {
    A: for(int i=0;i<data.numFeatures();) {
      columns_[i]=new ColumnStatistic(data.random_.nextInt(data.numColumns()), data.numClasses());
      for(int j=0;j<i;j++) if (columns_[i].column==columns_[j].column) continue A;  
      i++;
    }
  }

  /** Split classifier. Determines between the left or right subtree depending
   * on the given column and splitting value. 
   */
  static final class SplitClassifier implements Classifier {
    private static final long serialVersionUID = 6496848674571619538L;
    public final int column;
    public final double value;    
    
    SplitClassifier(SplitInfo i) { column = i.column; value = i.value; }    
    public int classify(DataAdapter data) { return data.toDouble(column)<=value? 0:1; }
    
    public int numClasses()  { return 2; }
    public String toString() { return "col="+column+", val="+value; }
  }
  
 private double get(int i, byte column) { data.seekToRow(i); return data.toDouble(column); }  
  //OJDK6
  public void sort(int[] a,byte column) {sort1(a, 0, a.length,column);}
  private void sort1(int x[], int off, int len,byte column) {
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
    if ((s = b-a) > 1)     sort1(x, off, s,column);
    if ((s = d-c) > 1)     sort1(x, n-s, s,column);
}
  private  int med3(int x[], int a, int b, int c,byte column) {
      return (get(x[a],column) < get(x[b],column) ?
      (get(x[b],column) < get(x[c],column) ? b : get(x[a],column) < get(x[c],column) ? c : a) :
      (get(x[b],column) > get(x[c],column) ? b : get(x[a],column) > get(x[c],column) ? c : a));
  }
  private  void swap(int x[], int a, int b) { int t=x[a];x[a]=x[b];x[b]=t; }
  private  void vecswap(int x[], int a, int b, int n) {
      for (int i=0; i<n; i++, a++, b++)  swap(x, a, b);
  }
}
